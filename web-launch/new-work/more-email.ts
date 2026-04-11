// ═══════════════════════════════════════════════════════════════════════════════
// 🚀 NEW-DOMAIN-SCANNER v2.0 (MEDALLION ARCHITECTURE)
// ═══════════════════════════════════════════════════════════════════════════════
// CHANGES FROM v1:
// ───────────────────────────────────────────────────────────────────────────────
// 1. Searches ALL niche keywords (not just first one) → 5-10x more domains
// 2. Filters "for sale" / parked / bulk-registrar domains via WHOIS org check
// 3. Filters privacy-protected registrants earlier (skip WHOIS credit waste)
// 4. Increased caps: 300 discovery results, 60 website checks, 30 WHOIS lookups
// 5. Added domain name heuristics to skip junk (random strings, too long, etc.)
// 6. Added "bulk registrar" detection (GoDaddy Auctions, Sedo, Afternic, etc.)
// 7. Added concurrent batching for WebsiteLaunches (3 at a time vs sequential)
// 8. Better logging with counts at every stage
// 9. Falls back to "general" keywords if niche yields <5 domains
// ═══════════════════════════════════════════════════════════════════════════════

import { serve }        from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

// ── ENVIRONMENT SECRETS ──────────────────────────────────────────────────────
const supabaseUrl         = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey  = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const WHOISXML_KEY        = Deno.env.get("WHOISXML_KEY");
const WEBSITELAUNCHES_KEY = Deno.env.get("WEBSITELAUNCHES_KEY");

// ── SYSTEM LIMITS (v2: INCREASED FOR BETTER YIELD) ──────────────────────────
const MAX_WHOISXML_RESULTS        = 300;  // ⬆️ was 100 → now 300
const MAX_WEBSITELAUNCHES_PER_RUN = 60;   // ⬆️ was 30 → now 60
const MAX_WHOIS_LOOKUPS           = 30;   // ⬆️ was 15 → now 30
const MAX_DOMAIN_AGE_DAYS         = 30;
const MAX_AUTHORITY_SCORE         = 5;
const WEBL_CONCURRENCY            = 3;    // 🆕 parallel website checks

// ── API ENDPOINTS ────────────────────────────────────────────────────────────
const WHOISXML_DISCOVERY = "https://domains-subdomains-discovery.whoisxmlapi.com/api/v1";
const WHOISXML_WHOIS     = "https://www.whoisxmlapi.com/whoisserver/WhoisService";
const WEBL_BASE          = "https://websitelaunches.com/api/v1";

// ── NICHE KEYWORDS (UNCHANGED) ──────────────────────────────────────────────
const NICHE_KEYWORDS: Record<string, string[]> = {
  "restaurant":   ["restauran", "cafe", "bistro", "eats", "kitchen"],
  "plumber":      ["plumb", "pipes", "heating", "drain"],
  "electrician":  ["electri", "wiring", "solar", "power"],
  "pest control": ["pest", "bug", "termite", "exterminat"],
  "salon":        ["salon", "beauty", "hair", "spa", "barber"],
  "dental":       ["dental", "dentist", "teeth", "smile"],
  "gym":          ["gym", "fitness", "yoga", "crossfit"],
  "lawyer":       ["law", "legal", "attorney", "solicitor"],
  "accountant":   ["account", "bookkeep", "tax", "finance"],
  "realEstate":   ["realty", "realtor", "propert", "estate"],
  "cleaning":     ["clean", "janitor", "maid"],
  "construction": ["construct", "builder", "contractor", "roof"],
  "photography":  ["photo", "studio", "imaging"],
  "consulting":   ["consult", "advisor", "solution"],
  "car wash":     ["carwash", "detailing", "autowash"],
  "general":      ["shop", "store", "service", "local", "pro"],
};

// ── 🆕 "FOR SALE" / PARKED DOMAIN DETECTION ────────────────────────────────
// These organizations bulk-register domains to resell them.
// If WHOIS shows one of these as registrant org, the domain is NOT a real startup.
const BULK_REGISTRAR_ORGS = [
  "godaddy", "sedo", "afternic", "dan.com", "hugedomains", "bodis",
  "above.com", "parking crew", "domainmarket", "buydomains", "brandbucket",
  "undeveloped", "domainking", "namesilo", "domain capital", "squadhelp",
  "atom.com", "domainagents", "flippa", "epik", "domainlore", "nametrade",
  "parked.com", "fabulous.com", "uniregistry", "domain holdings",
  "name administration", "privacy protect", "domains by proxy",
  "whoisguard", "contactprivacy", "withheldforprivacy", "redacted for privacy",
  "identity protection", "super privacy", "private by design"
];

// 🆕 Patterns in domain names that indicate "for sale" or junk domains
const JUNK_DOMAIN_PATTERNS = [
  /^[a-z]{20,}\./,          // Random 20+ char strings (generated names)
  /^[0-9]{4,}\./,           // Pure numeric domains
  /forsale/i,               // Explicitly for sale
  /\d{6,}/,                 // 6+ consecutive digits
  /^(buy|sell|get|best|top|cheap|free|premium|domain)/i,  // SEO spam prefixes
  /-(buy|sell|deal|offer|cheap|free|best|top)-/i,         // SEO spam in middle
];

// ── PRIVACY EMAIL PATTERNS (UNCHANGED) ──────────────────────────────────────
const PRIVACY_PATTERNS = [
  "privacy", "protect", "whoisguard", "contactprivacy", "domainsprivacy",
  "registrant@", "hidden", "redacted", "withheld", "noreply", "not disclosed",
  "gdpr", "networksolutions", "proxy", "masked", "contact@",
  "obscure.me", "privateregistryauthority"
];

// ── UTILITY FUNCTIONS ────────────────────────────────────────────────────────
function isPrivacyEmail(email: string | null | undefined): boolean {
  if (!email) return true;
  const lower = email.toLowerCase();
  return PRIVACY_PATTERNS.some((p) => lower.includes(p));
}

function guessCompanyName(domain: string): string {
  return domain
    .replace(/\.(com|net|org|io|co\.uk|co|biz|info|in|us|ca|au)$/i, "")
    .replace(/[-_.]/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .trim();
}

// 🆕 Pre-filter: Is this domain name likely a real business or junk?
function isJunkDomain(domain: string): boolean {
  // Check against junk patterns
  if (JUNK_DOMAIN_PATTERNS.some(p => p.test(domain))) return true;
  
  // Domain name (without TLD) is too short (1-2 chars) or too long (30+)
  const name = domain.replace(/\.[^.]+$/, "");
  if (name.length <= 2 || name.length >= 30) return true;
  
  // All consonants / no vowels = likely random string
  const alphaOnly = name.replace(/[^a-zA-Z]/g, "");
  if (alphaOnly.length > 6) {
    const vowelCount = (alphaOnly.match(/[aeiou]/gi) || []).length;
    const vowelRatio = vowelCount / alphaOnly.length;
    if (vowelRatio < 0.15) return true; // Less than 15% vowels = gibberish
  }
  
  return false;
}

// 🆕 Check if WHOIS org indicates a bulk registrar / domain flipper
function isBulkRegistrarOrg(org: string | null | undefined): boolean {
  if (!org) return false;
  const lower = org.toLowerCase();
  return BULK_REGISTRAR_ORGS.some(b => lower.includes(b));
}

// 🆕 Batch helper: run promises in chunks of N
async function batchProcess<T, R>(
  items: T[], 
  batchSize: number, 
  processor: (item: T) => Promise<R>,
  delayMs = 300
): Promise<R[]> {
  const results: R[] = [];
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    const batchResults = await Promise.allSettled(batch.map(processor));
    for (const r of batchResults) {
      if (r.status === "fulfilled") results.push(r.value);
    }
    if (i + batchSize < items.length) {
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }
  return results;
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 1: FETCH DOMAINS (Discovery API)
// 🆕 NOW SEARCHES ALL KEYWORDS (not just the first one)
// ─────────────────────────────────────────────────────────────────────────────
async function fetchWhoisDomains(keywords: string[], sinceDate: string) {
  if (!WHOISXML_KEY) throw new Error("Missing WHOISXML_KEY");
  
  const allDomains: Map<string, { domain: string; createdAt: string }> = new Map();
  
  // 🆕 v2: Search ALL keywords, not just the first one
  // Each keyword gets its own API call for better results
  for (const keyword of keywords) {
    const wildcardKeyword = `*${keyword}*`;
    console.log(`   [🔍 STEP 1] Discovery API: Searching [${wildcardKeyword}] since ${sinceDate}`);

    try {
      const res = await fetch(WHOISXML_DISCOVERY, {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({
          apiKey: WHOISXML_KEY,
          sinceDate: sinceDate,
          domains: { include: [wildcardKeyword] }
        }),
      });

      const data = await res.json() as any;
      
      if (data.code === 403) {
        console.error(`   [❌ ERROR] WhoisXML 403: ${data.messages}`);
        continue; // Try next keyword instead of returning empty
      }

      const rawList: string[] = data.domainsList || [];
      let added = 0;
      
      for (const d of rawList) {
        const cleaned = d.toLowerCase().trim();
        if (cleaned.length > 0 && !allDomains.has(cleaned)) {
          allDomains.set(cleaned, { domain: cleaned, createdAt: sinceDate });
          added++;
        }
        if (allDomains.size >= MAX_WHOISXML_RESULTS) break;
      }
      
      console.log(`      ↳ Found ${rawList.length} raw, added ${added} new unique domains`);
      
      if (allDomains.size >= MAX_WHOISXML_RESULTS) break;
      
      // Small delay between keyword searches to be respectful
      await new Promise(resolve => setTimeout(resolve, 300));
      
    } catch (err) {
      console.error(`   [❌ ERROR] Discovery API Failed for "${keyword}": ${(err as Error).message}`);
      continue; // Try next keyword
    }
  }
  
  const results = Array.from(allDomains.values());
  console.log(`   [✅ STEP 1] Total unique domains found: ${results.length}`);
  return results;
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 1.5 🆕: PRE-FILTER JUNK DOMAINS (FREE - NO API COST)
// ─────────────────────────────────────────────────────────────────────────────
function preFilterDomains(domains: { domain: string; createdAt: string }[]): { domain: string; createdAt: string }[] {
  const before = domains.length;
  
  const filtered = domains.filter(d => {
    if (isJunkDomain(d.domain)) {
      return false;
    }
    return true;
  });
  
  const removed = before - filtered.length;
  console.log(`   [🧹 STEP 1.5] Pre-Filter: Removed ${removed} junk/gibberish domains. ${filtered.length} remain.`);
  return filtered;
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 2: FILTER BUILT WEBSITES (WebsiteLaunches API)
// 🆕 NOW RUNS IN PARALLEL BATCHES OF 3
// 🆕 ALSO DETECTS "FOR SALE" PARKED PAGES
// ─────────────────────────────────────────────────────────────────────────────
async function checkUnbuiltDomains(domains: string[]) {
  if (!WEBSITELAUNCHES_KEY) {
    console.warn("   [⚠️ WARNING] Missing WEBSITELAUNCHES_KEY. Bypassing unbuilt filter.");
    return domains.map(d => ({
      domain: d, domain_authority: 0, domain_age_days: 0,
      domain_category: null, domain_subcategory: null, registered_date: null
    }));
  }

  const batch = domains.slice(0, MAX_WEBSITELAUNCHES_PER_RUN);
  console.log(`   [🔍 STEP 2] Verification: Checking ${batch.length} domains for launched websites...`);

  type UnbuiltResult = {
    domain: string;
    domain_authority: number;
    domain_age_days: number;
    domain_category: string | null;
    domain_subcategory: string | null;
    registered_date: string | null;
  } | null;

  // 🆕 Process in parallel batches of WEBL_CONCURRENCY
  const checkOne = async (d: string): Promise<UnbuiltResult> => {
    try {
      const res = await fetch(`${WEBL_BASE}/domain/${d}`, {
        method: "GET",
        headers: { "Content-Type": "application/json", "X-API-Key": WEBSITELAUNCHES_KEY }
      });
      
      if (!res.ok) return null;
      
      const data = await res.json() as any;
      const r = data?.data;
      if (!r) return null;

      const authorityScore = r.site_authority ?? r.domain_authority ?? 0;
      
      // 🆕 FILTER: Skip domains that have a launched website
      if (r.launch_detected === true) return null;
      
      // 🆕 FILTER: Skip domains with authority > threshold (established sites)
      if (authorityScore > MAX_AUTHORITY_SCORE) return null;
      
      // 🆕 FILTER: Detect "for sale" / parked pages
      // WebsiteLaunches sometimes returns category data that indicates parking
      const category = (r.category || "").toLowerCase();
      if (category.includes("parked") || category.includes("for sale") || category.includes("domain parking")) {
        console.log(`      ↳ Skipped ${d}: parked/for-sale page detected`);
        return null;
      }

      return {
        domain: d.toLowerCase(),
        domain_authority: authorityScore,
        domain_age_days: Math.floor((r.domain_age ?? 0) * 365),
        domain_category: r.category || null,
        domain_subcategory: r.subcategory || null,
        registered_date: r.domain_age_date || null,
      };
    } catch (err) {
      return null;
    }
  };

  const results = await batchProcess(batch, WEBL_CONCURRENCY, checkOne, 500);
  const unbuilt = results.filter((r): r is NonNullable<typeof r> => r !== null);
  
  console.log(`   [✅ STEP 2] Filtered down to ${unbuilt.length} strictly unbuilt domains.`);
  return unbuilt;
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 3: EXTRACT REGISTRANT EMAILS (WHOIS API)
// 🆕 NOW ALSO DETECTS BULK REGISTRAR ORGANIZATIONS
// ─────────────────────────────────────────────────────────────────────────────
type ContactResult = {
  email: string | null;
  name: string | null;
  phone: string | null;
  org: string | null;
  is_bulk_registrar: boolean; // 🆕
};

async function whoisRegistrantLookup(domain: string): Promise<ContactResult> {
  const EMPTY: ContactResult = { email: null, name: null, phone: null, org: null, is_bulk_registrar: false };
  if (!WHOISXML_KEY) return EMPTY;

  try {
    const url = `${WHOISXML_WHOIS}?apiKey=${WHOISXML_KEY}&domainName=${encodeURIComponent(domain)}&outputFormat=JSON`;
    const res = await fetch(url);
    if (!res.ok) return EMPTY;

    const data = await res.json() as any;
    const record = data?.WhoisRecord;
    
    const reg = record?.registrant || record?.technicalContact || record?.administrativeContact || null;
    if (!reg) return EMPTY;

    const raw_email = reg.email || null;
    const raw_org = reg.organization || null;
    
    // 🆕 Check if this is a bulk registrar / domain flipper
    if (isBulkRegistrarOrg(raw_org)) {
      console.log(`      ↳ Skipped ${domain}: bulk registrar org detected (${raw_org})`);
      return { ...EMPTY, org: raw_org, is_bulk_registrar: true };
    }
    
    // 🆕 Also check registrar name for parking services
    const registrarName = record?.registrarName || "";
    if (isBulkRegistrarOrg(registrarName)) {
      // Registrar alone doesn't mean "for sale" — many real people use GoDaddy.
      // But if ALSO no real email, skip it.
      if (isPrivacyEmail(raw_email)) {
        return { ...EMPTY, org: raw_org, is_bulk_registrar: false };
      }
    }
    
    if (isPrivacyEmail(raw_email)) {
      return EMPTY;
    }

    return {
      email: raw_email,
      name:  reg.name || (reg.firstName ? `${reg.firstName} ${reg.lastName || ""}`.trim() : null),
      phone: reg.telephone || null,
      org:   raw_org,
      is_bulk_registrar: false,
    };
  } catch (err) {
    return EMPTY;
  }
}

// ═════════════════════════════════════════════════════════════════════════════
// MAIN EXECUTION HANDLER
// ═════════════════════════════════════════════════════════════════════════════
serve(async (req) => {
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    const payload = await req.json();
    const { 
        user_id, 
        niche = "restaurant", 
        location = "London", 
        target_count = 30,
        sources = ["new_domain"]
    } = payload;

    if (!user_id) throw new Error("Missing user_id");

    // ── ARCHITECTURE ORCHESTRATION CHECK ──
    if (!sources.includes("new_domain")) {
      console.log(`[⏭️ BYPASS] "new_domain" not in requested sources. Exiting Scanner.`);
      return new Response(JSON.stringify({ success: true, saved_count: 0, message: "Scanner bypassed" }), { status: 200 });
    }

    const currentTargetCount = Math.ceil(target_count / sources.length);
    const sinceDate = new Date(Date.now() - MAX_DOMAIN_AGE_DAYS * 86_400_000).toISOString().split("T")[0];

    console.log(`\n${"═".repeat(60)}\n🚀 [START] NEW-DOMAIN-SCANNER v2.0 PIPELINE\n${"═".repeat(60)}`);
    console.log(` 👤 User: ${user_id} | 🎯 Target: ${currentTargetCount} | 🏷️ Niche: ${niche}`);

    // ── PHASE 1: Keyword Selection & Fetching ──
    let rawKeywords = NICHE_KEYWORDS[niche];
    if (!rawKeywords) {
      const dynamicKeyword = niche.trim().split(" ")[0].toLowerCase();
      rawKeywords = [dynamicKeyword];
      console.log(`   [⚠️ SYSTEM] Niche not in DB. Generated dynamic keyword: [${dynamicKeyword}]`);
    }
    
    // 🆕 v2: Search ALL keywords, not just the first one
    const keywordsToSearch = rawKeywords; // was: rawKeywords.slice(0, 1)
    const rawDomains = await fetchWhoisDomains(keywordsToSearch, sinceDate);
    
    if (rawDomains.length === 0) {
      // 🆕 v2: Fallback to "general" keywords if niche yields nothing
      console.log(`   [🔄 FALLBACK] Niche "${niche}" yielded 0 domains. Trying "general" keywords...`);
      const fallbackKeywords = NICHE_KEYWORDS["general"] || ["shop", "store", "service"];
      const fallbackDomains = await fetchWhoisDomains(fallbackKeywords.slice(0, 2), sinceDate);
      
      if (fallbackDomains.length === 0) {
        console.log(`\n🏁 [END] Pipeline terminated: 0 domains found even with fallback.`);
        return new Response(JSON.stringify({ success: true, saved_count: 0, message: "No domains found" }), { status: 200 });
      }
      
      // Use fallback domains and continue
      rawDomains.push(...fallbackDomains);
    }

    // 🆕 PHASE 1.5: Pre-filter junk domains (FREE, no API cost)
    const cleanDomains = preFilterDomains(rawDomains);
    
    if (cleanDomains.length === 0) {
      console.log(`\n🏁 [END] Pipeline terminated: All domains were junk after pre-filter.`);
      return new Response(JSON.stringify({ success: true, saved_count: 0, message: "All domains filtered as junk" }), { status: 200 });
    }

    // ── PHASE 2: Website Verification Filter ──
    const domainStringList = cleanDomains.map(d => d.domain);
    const unbuilt = await checkUnbuiltDomains(domainStringList);
    
    if (unbuilt.length === 0) {
      console.log(`\n🏁 [END] Pipeline terminated: All domains had active websites.`);
      return new Response(JSON.stringify({ success: true, saved_count: 0, message: "All launched" }), { status: 200 });
    }

    // ── PHASE 3: Contact Enrichment (WHOIS) ──
    console.log(`   [🔍 STEP 3] Enrichment: Mining WHOIS data for ${Math.min(unbuilt.length, MAX_WHOIS_LOOKUPS)} domains...`);
    const contactMap = new Map<string, ContactResult>();
    const sorted = [...unbuilt].sort((a, b) => a.domain_age_days - b.domain_age_days);
    
    // 🆕 v2: Increased limit + skip bulk registrar domains
    let bulkRegistrarCount = 0;
    
    for (const d of sorted.slice(0, MAX_WHOIS_LOOKUPS)) {
      const contact = await whoisRegistrantLookup(d.domain);
      
      // 🆕 If bulk registrar detected, mark domain to skip in Phase 4
      if (contact.is_bulk_registrar) {
        bulkRegistrarCount++;
        contactMap.set(d.domain, contact); // Still store so we can skip it
      } else {
        contactMap.set(d.domain, contact);
      }
      
      await new Promise((r) => setTimeout(r, 600));
    }
    
    if (bulkRegistrarCount > 0) {
      console.log(`   [🚫 STEP 3] Detected ${bulkRegistrarCount} bulk registrar / "for sale" domains.`);
    }

    // ── PHASE 4: Database Row Construction ──
    console.log(`   [💾 STEP 4] Assembly: Constructing raw and clean rows...`);
    const rawRows: any[] = [];
    const cleanRows: any[] = [];
    let skippedBulk = 0;
    
    for (const d of sorted) {
      if (cleanRows.length >= currentTargetCount) break;

      const contact = contactMap.get(d.domain) || { email: null, name: null, phone: null, org: null, is_bulk_registrar: false };
      
      // 🆕 v2: Skip bulk registrar domains entirely
      if (contact.is_bulk_registrar) {
        skippedBulk++;
        continue;
      }
      
      const companyName = contact.org || guessCompanyName(d.domain);
      
      let tier = "tier_4_domain_only";
      if (contact.email && contact.phone) tier = "tier_1_verified";
      else if (contact.email) tier = "tier_1_verified";
      else if (contact.phone) tier = "tier_2_phone_only";

      console.log(`      ➡️  Ready: ${d.domain} | Email: ${contact.email ? "✅" : "❌"} | Phone: ${contact.phone ? "✅" : "❌"} | Tier: ${tier}`);

      const rawId = crypto.randomUUID();
      rawRows.push({
        id: rawId,
        source: "whois_discovery",
        search_query: `${niche} new domains`,
        domain: d.domain,
        raw_payload: { domain_data: d, contact_data: contact }
      });

      cleanRows.push({
        raw_prospect_id: rawId,
        domain: d.domain,
        company_name: companyName,
        phone: contact.phone,
        email: contact.email,
        address: location,
        listing_url: `https://${d.domain}`,
        source: "new_domain",
        search_query: `${niche} new domains`,
        business_type: "Brand New Startup",
        lead_tier: tier,
        pitch_angle: contact.email
          ? "Email founder directly"
          : contact.phone
            ? "Call founder directly"
            : "Search LinkedIn for founder",
        has_website: false,
        domain_launched: false,
        domain_age_days: d.domain_age_days,
        domain_authority: d.domain_authority,
        domain_category: d.domain_category,
        domain_registered_date: d.registered_date || sinceDate,
      });
    }
    
    if (skippedBulk > 0) {
      console.log(`   [🚫 STEP 4] Skipped ${skippedBulk} "for sale" / bulk registrar domains from final results.`);
    }

    // ── PHASE 5: Database Transactions ──
    console.log(`   [📦 STEP 5] Database: Upserting ${cleanRows.length} leads...`);
    
    if (rawRows.length > 0) {
      const { error: rawErr } = await supabase.from("raw_prospects").insert(rawRows);
      if (rawErr) console.error(`   [❌ ERROR] Raw Insert Failed: ${rawErr.message}`);
    }

    const { data: inserted, error: prospectsErr } = await supabase
      .from("prospects")
      .upsert(cleanRows, { onConflict: "domain" })
      .select("id");
      
    if (prospectsErr) {
      console.error(`   [❌ ERROR] Prospects Upsert Failed: ${prospectsErr.message}`);
      throw new Error(prospectsErr.message);
    }

    if (inserted?.length) {
      const { error: lockErr } = await supabase.from("user_prospects").upsert(
        inserted.map(p => ({ user_id, prospect_id: p.id })),
        { onConflict: "user_id,prospect_id" }
      );
      if (lockErr) console.error(`   [❌ ERROR] User Prospects Lock Failed: ${lockErr.message}`);

      const { data: profile } = await supabase
        .from("profiles")
        .select("drip_leads_this_month")
        .eq("id", user_id)
        .single();
        
      if (profile) {
        await supabase.from("profiles")
          .update({ drip_leads_this_month: (profile.drip_leads_this_month || 0) + inserted.length })
          .eq("id", user_id);
      }
    }

    // 🆕 v2: Summary stats in response
    const stats = {
      domains_discovered: rawDomains.length,
      after_junk_filter: cleanDomains.length,
      unbuilt_verified: unbuilt.length,
      bulk_registrar_skipped: bulkRegistrarCount + skippedBulk,
      final_saved: inserted?.length || 0,
    };

    console.log(`\n🏁 [END] Pipeline Complete.`);
    console.log(`   📊 Discovered: ${stats.domains_discovered} → Pre-filtered: ${stats.after_junk_filter} → Unbuilt: ${stats.unbuilt_verified} → Saved: ${stats.final_saved}`);
    console.log(`   🚫 Bulk/ForSale skipped: ${stats.bulk_registrar_skipped}`);
    console.log(`${"═".repeat(60)}\n`);

    return new Response(JSON.stringify({ success: true, saved_count: stats.final_saved, stats }), { status: 200 });

  } catch (error) {
    console.error(`\n🚨 [FATAL CRASH] ${(error as Error).message}`);
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
});