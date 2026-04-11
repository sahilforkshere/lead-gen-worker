// ═══════════════════════════════════════════════════════════════════════════════
// 🚀 THE ULTIMATE EDGE FUNCTION: new-domain-scanner (PRODUCTION MASTER)
// ═══════════════════════════════════════════════════════════════════════════════
// This script handles the entire "New Domain" lead generation pipeline:
// 1. Fetches domains from the last 30 days (Discovery API)
// 2. Proves they have no website built (WebsiteLaunches API)
// 3. Extracts the founder's email & phone (WHOIS Registry API)
// 4. Upserts to `leads`, locks via `user_leads`, and updates quotas.
// ═══════════════════════════════════════════════════════════════════════════════

import { serve }        from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

// ── ENVIRONMENT SECRETS ──────────────────────────────────────────────────────
const supabaseUrl         = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey  = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const WHOISXML_KEY        = Deno.env.get("WHOISXML_KEY");
const WEBSITELAUNCHES_KEY = Deno.env.get("WEBSITELAUNCHES_KEY");

// ── SYSTEM LIMITS & THRESHOLDS (Protecting your API budgets) ─────────────────
const MAX_WHOISXML_RESULTS        = 100; // Cap Discovery API domains processed
const MAX_WEBSITELAUNCHES_PER_RUN = 30;  // Protect Free Tier (3000/mo)
const MAX_WHOIS_LOOKUPS           = 15;  // Protect WHOIS credits
const MAX_DOMAIN_AGE_DAYS         = 30;  // Lookback window for new domains
const MAX_AUTHORITY_SCORE         = 5;   // Score <= 5 guarantees an unbuilt site

// ── API ENDPOINTS ────────────────────────────────────────────────────────────
const WHOISXML_DISCOVERY = "https://domains-subdomains-discovery.whoisxmlapi.com/api/v1";
const WHOISXML_WHOIS     = "https://www.whoisxmlapi.com/whoisserver/WhoisService";
const WEBL_BASE          = "https://websitelaunches.com/api/v1";

// ── THE KNOWLEDGE ENGINE ─────────────────────────────────────────────────────
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

// Filters out useless proxy emails so you only get real founder emails
const PRIVACY_PATTERNS = [
  "privacy", "protect", "whoisguard", "contactprivacy", "domainsprivacy",
  "registrant@", "hidden", "redacted", "withheld", "noreply", "not disclosed",
  "gdpr", "networksolutions", "proxy", "masked", "contact@"
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

// ─────────────────────────────────────────────────────────────────────────────
// STEP 1: FETCH DOMAINS (Discovery API)
// ─────────────────────────────────────────────────────────────────────────────
async function fetchWhoisDomains(keywords: string[], sinceDate: string) {
  if (!WHOISXML_KEY) throw new Error("Missing WHOISXML_KEY");
  
  const wildcardKeywords = keywords.map(k => `*${k}*`);
  console.log(`   [🔍 STEP 1] Discovery API: Searching [${wildcardKeywords.join(", ")}] since ${sinceDate}`);

  try {
    const res = await fetch(WHOISXML_DISCOVERY, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ apiKey: WHOISXML_KEY, sinceDate: sinceDate, domains: { include: wildcardKeywords } }),
    });

    const data = await res.json() as any;
    
    // Safety Net: Catch out-of-credits error
    if (data.code === 403) {
        console.error(`   [❌ ERROR] WhoisXML 403: ${data.messages}`);
        return [];
    }

    const rawList: string[] = data.domainsList || [];
    const validDomains = rawList.filter((d) => d.trim().length > 0).slice(0, MAX_WHOISXML_RESULTS);
    
    console.log(`   [✅ STEP 1] Success: Found ${validDomains.length} domains matching criteria.`);
    return validDomains.map((domain) => ({ domain: domain.toLowerCase().trim(), createdAt: sinceDate }));
  } catch (err) {
    console.error(`   [❌ ERROR] Discovery API Fetch Failed: ${(err as Error).message}`);
    return [];
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 2: FILTER BUILT WEBSITES (WebsiteLaunches API)
// ─────────────────────────────────────────────────────────────────────────────
async function checkUnbuiltDomains(domains: string[]) {
  if (!WEBSITELAUNCHES_KEY) {
    console.warn("   [⚠️ WARNING] Missing WEBSITELAUNCHES_KEY. Bypassing unbuilt filter.");
    return domains.map(d => ({ domain: d, domain_authority: 0, domain_age_days: 0, domain_category: null, domain_subcategory: null, registered_date: null }));
  }

  const batch = domains.slice(0, MAX_WEBSITELAUNCHES_PER_RUN);
  const unbuilt = [];
  console.log(`   [🔍 STEP 2] Verification: Checking ${batch.length} domains for launched websites...`);

  for (const d of batch) {
    try {
      const res = await fetch(`${WEBL_BASE}/domain/${d}`, {
        method: "GET",
        headers: { "Content-Type": "application/json", "X-API-Key": WEBSITELAUNCHES_KEY }
      });
      
      if (!res.ok) continue; 
      
      const data = await res.json() as any;
      const r = data?.data; 
      if (!r) continue;

      const authorityScore = r.site_authority ?? r.domain_authority ?? 0;
      
      // The Golden Rule: Must not have launched AND must have basically zero authority
      if (r.launch_detected === false && authorityScore <= MAX_AUTHORITY_SCORE) {
        unbuilt.push({
          domain: (d || "").toLowerCase(),
          domain_authority: authorityScore,
          domain_age_days: Math.floor((r.domain_age ?? 0) * 365),
          domain_category: r.category || null,
          domain_subcategory: r.subcategory || null,
          registered_date: r.domain_age_date || null,
        });
      }
      
      // Delay to protect your API limits from being flagged as a bot
      await new Promise(resolve => setTimeout(resolve, 500));
    } catch (err) { }
  }
  
  console.log(`   [✅ STEP 2] Success: Filtered down to ${unbuilt.length} strictly unbuilt domains.`);
  return unbuilt;
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 3: EXTRACT REGISTRANT EMAILS (WHOIS API)
// ─────────────────────────────────────────────────────────────────────────────
async function whoisRegistrantLookup(domain: string) {
  const EMPTY = { email: null, name: null, phone: null, org: null };
  if (!WHOISXML_KEY) return EMPTY;

  try {
    const url = `${WHOISXML_WHOIS}?apiKey=${WHOISXML_KEY}&domainName=${encodeURIComponent(domain)}&outputFormat=JSON`;
    const res = await fetch(url);
    if (!res.ok) return EMPTY;

    const data = await res.json() as any;
    const record = data?.WhoisRecord;
    
    // Prioritize the registrant, but fallback to technical contact if needed
    const reg = record?.registrant || record?.technicalContact || record?.administrativeContact || null;
    if (!reg) return EMPTY;

    const raw_email = reg.email || null;
    
    // Drop the lead if the founder hid behind GoDaddy Privacy Guard
    if (isPrivacyEmail(raw_email)) {
       return EMPTY;
    }

    return {
      email: raw_email,
      name:  reg.name || (reg.firstName ? `${reg.firstName} ${reg.lastName || ""}`.trim() : null),
      phone: reg.telephone || null,
      org:   reg.organization || null,
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
        sources = ["new_domain"] // Supports Orchestrator pattern array
    } = payload;

    if (!user_id) throw new Error("Missing user_id");

    // ── ARCHITECTURE ORCHESTRATION CHECK ──
    // If the user requested leads, but DID NOT request "new_domain" leads, exit gracefully.
    if (!sources.includes("new_domain")) {
        console.log(`[⏭️ BYPASS] "new_domain" not in requested sources. Exiting Scanner.`);
        return new Response(JSON.stringify({ success: true, saved_count: 0, message: "Scanner bypassed" }), { status: 200 });
    }

    // Determine target slice (if they want multiple sources, we split the target count)
    const currentTargetCount = Math.ceil(target_count / sources.length);
    const sinceDate = new Date(Date.now() - MAX_DOMAIN_AGE_DAYS * 86_400_000).toISOString().split("T")[0];

    console.log(`\n${"═".repeat(60)}\n🚀 [START] NEW-DOMAIN-SCANNER PIPELINE\n${"═".repeat(60)}`);
    console.log(` 👤 User: ${user_id} | 🎯 Target: ${currentTargetCount} | 🏷️ Niche: ${niche}`);

    // ── PHASE 1: Keyword Selection & Fetching ──
    let rawKeywords = NICHE_KEYWORDS[niche];
    if (!rawKeywords) {
      const dynamicKeyword = niche.trim().split(" ")[0].toLowerCase();
      rawKeywords = [dynamicKeyword];
      console.log(`   [⚠️ SYSTEM] Niche not in DB. Generated dynamic keyword: [${dynamicKeyword}]`);
    }
    
    // We only use the FIRST keyword to avoid the "AND" zero-result bug in WhoisXML
    const keywordsToSearch = rawKeywords.slice(0, 1);
    const rawDomains = await fetchWhoisDomains(keywordsToSearch, sinceDate);
    
    if (rawDomains.length === 0) {
      console.log(`\n🏁 [END] Pipeline terminated early: 0 domains found.`);
      return new Response(JSON.stringify({ success: true, saved_count: 0, message: "No domains found" }), { status: 200 });
    }

    // ── PHASE 2: Website Verification Filter ──
    const domainStringList = rawDomains.map(d => d.domain);
    const unbuilt = await checkUnbuiltDomains(domainStringList);
    
    if (unbuilt.length === 0) {
      console.log(`\n🏁 [END] Pipeline terminated early: All domains had active websites.`);
      return new Response(JSON.stringify({ success: true, saved_count: 0, message: "All launched" }), { status: 200 });
    }

    // ── PHASE 3: Contact Enrichment (WHOIS) ──
    console.log(`   [🔍 STEP 3] Enrichment: Mining WHOIS registration data for founder contacts...`);
    const contactMap = new Map();
    const sorted = [...unbuilt].sort((a, b) => a.domain_age_days - b.domain_age_days); // prioritize newest
    
    for (const d of sorted.slice(0, MAX_WHOIS_LOOKUPS)) {
      const contact = await whoisRegistrantLookup(d.domain);
      contactMap.set(d.domain, contact);
      await new Promise((r) => setTimeout(r, 600)); // Respect WHOIS API limits
    }

    // ── PHASE 4: Database Row Construction ──
    console.log(`   [💾 STEP 4] Assembly: Constructing database rows...`);
    const leadRows = [];
    
    for (const d of sorted) {
      if (leadRows.length >= currentTargetCount) break; // Stop if we hit user target

      const contact = contactMap.get(d.domain) || { email: null, name: null, phone: null, org: null };
      const companyName = contact.org || guessCompanyName(d.domain);
      
      // Tier classification for frontend badges
      let tier = "tier_4_domain_only";
      if (contact.email) tier = "tier_1_verified";
      else if (contact.phone) tier = "tier_2_phone";

      console.log(`      ➡️  Ready: ${d.domain} | Email: ${contact.email ? "✅ YES" : "❌ NO"}`);

      leadRows.push({
        // Top-Level Schema Requirements
        domain: d.domain,
        company_name: companyName,
        phone: contact.phone,
        email: contact.email,
        address: location,
        listing_url: `https://${d.domain}`,
        source: "new_domain",
        drip_source: "cold_drip",
        search_query: `${niche} new domains`,
        has_website: false,
        status: "pending",
        
        // Intelligence Schema Requirements
        domain_launched: false,
        domain_age_days: d.domain_age_days,
        domain_authority: d.domain_authority,
        domain_category: d.domain_category,
        domain_subcategory: d.domain_subcategory,
        domain_registered_date: d.registered_date || sinceDate,
        
        // JSONB Meta Schema
        lead_data: {
          company_name: companyName,
          domain: d.domain,
          email: contact.email,
          phone: contact.phone,
          tier: tier,
          contact_strategy: contact.email ? ["1. Email founder directly"] : ["1. Search LinkedIn for founder"],
        },
      });
    }

    // ── PHASE 5: Database Transactions ──
    console.log(`   [📦 STEP 5] Database: Upserting to Supabase...`);
    
    // 1. Insert into Global Leads Pool
    const { data: inserted, error: leadsErr } = await supabase.from("leads").upsert(leadRows, { onConflict: "domain" }).select("id");
    if (leadsErr) {
        console.error(`   [❌ ERROR] Database Leads Upsert Failed: ${leadsErr.message}`);
        throw new Error(leadsErr.message);
    }

    if (inserted?.length) {
      // 2. Lock the leads for this specific user
      const { error: lockErr } = await supabase.from("user_leads").upsert(
          inserted.map(l => ({ user_id, lead_id: l.id })), 
          { onConflict: "user_id,lead_id" }
      );
      if (lockErr) console.error(`   [❌ ERROR] User Leads Lock Failed: ${lockErr.message}`);

      // 3. Update their monthly profile quota
      const { data: profile } = await supabase.from("profiles").select("drip_leads_this_month").eq("id", user_id).single();
      if (profile) {
        await supabase.from("profiles")
          .update({ drip_leads_this_month: (profile.drip_leads_this_month || 0) + inserted.length })
          .eq("id", user_id);
      }
    }

    console.log(`\n🏁 [END] Pipeline Complete. Successfully saved ${inserted?.length || 0} leads.`);
    console.log(`${"═".repeat(60)}\n`);

    return new Response(JSON.stringify({ success: true, saved_count: inserted?.length || 0 }), { status: 200 });

  } catch (error) {
    console.error(`\n🚨 [FATAL CRASH] ${(error as Error).message}`);
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
});