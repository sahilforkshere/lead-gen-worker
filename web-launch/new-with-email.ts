// ═══════════════════════════════════════════════════════════════════════════════
// EDGE FUNCTION: new-domain-scanner
//
// FILE: supabase/functions/new-domain-scanner/index.ts
// DEPLOY: supabase functions deploy new-domain-scanner
//
// SECRETS:
//   supabase secrets set WHOISXML_KEY=at_ZnqP2paGe3KwdQOHlqvkIBqEFSjV0
//   supabase secrets set WEBSITELAUNCHES_KEY=your_key
//   supabase secrets set HUNTER_API_KEY=your_key   (optional — only used for 30+ day old domains)
//
// ── FIXES FROM PREVIOUS VERSION ──────────────────────────────────────────────
//   BUG 1: Wrong WhoisXML endpoint — was using Domains & Subdomains Discovery
//          (for finding subdomains of existing sites). Now uses the correct
//          Newly Registered Domains API.
//
//   BUG 2: Hunter.io called on brand new domains — always returns 0 results
//          because Hunter crawls indexed content and a 3-day-old domain has none.
//          Replaced with WhoisXML WHOIS lookup which returns registrant email
//          directly from the registration record. Same API key, no new secret.
//
//   BUG 3: sinceDate too narrow — combined with single keyword caused 0 results.
//          Now uses 30-day window + fallback to broader search if 0 returned.
//
// ── CREDIT SPEND PER RUN ────────────────────────────────────────────────────
//   WhoisXML Newly Registered Domains : 1 request = up to 100 domains
//   WhoisXML WHOIS lookup             : 1 credit per domain (same account)
//     → Max 10 WHOIS lookups per run to protect free credits
//   websitelaunches.com               : 1 call per domain (free tier: 3000/mo)
//     → Capped at 25 per run
//   Hunter.io                         : SKIP for new domains. Only use if domain
//     → is 30+ days old (not in this pipeline)
// ═══════════════════════════════════════════════════════════════════════════════

import { serve }        from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl         = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey  = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const WHOISXML_KEY        = Deno.env.get("WHOISXML_KEY")        || "";
const WEBSITELAUNCHES_KEY = Deno.env.get("WEBSITELAUNCHES_KEY") || "";
const HUNTER_KEY          = Deno.env.get("HUNTER_API_KEY")      || ""; // optional

// ── Credit guards ─────────────────────────────────────────────────────────────
const MAX_DOMAINS_PER_RUN    = 100;  // WhoisXML domains fetched per run
const MAX_WEBL_PER_RUN       = 25;   // websitelaunches single lookups per run
const MAX_WHOIS_LOOKUPS      = 10;   // WHOIS registrant lookups per run (costs credits)
const MAX_DOMAIN_AGE_DAYS    = 30;   // look back 30 days
const MAX_AUTHORITY_SCORE    = 5;    // 0–5 = brand new, no web history

// ── API endpoints ─────────────────────────────────────────────────────────────
// ✅ FIX 1: Correct endpoint for newly registered domains
const WHOISXML_NEWREG  = "https://newly-registered-domains.whoisxmlapi.com/api/v1";
const WHOISXML_WHOIS   = "https://www.whoisxmlapi.com/whoisserver/WhoisService";
const WEBL_BASE        = "https://websitelaunches.com/api/v1";

// ── Niche keyword map ─────────────────────────────────────────────────────────
// Short keywords work better — WhoisXML does substring matching on domain names
const NICHE_KEYWORDS: Record<string, string[]> = {
  restaurant:   ["restaurant", "cafe", "bistro"],
  plumber:      ["plumb", "drain", "heating"],
  electrician:  ["electri", "wiring", "solar"],
  salon:        ["salon", "beauty", "hair"],
  dental:       ["dental", "dentist", "smile"],
  gym:          ["gym", "fitness", "yoga"],
  lawyer:       ["legal", "attorney", "solicitor"],
  accountant:   ["account", "bookkeep", "taxadvi"],
  realEstate:   ["realty", "propert", "estate"],
  cleaning:     ["clean", "janitor", "housekeep"],
  construction: ["construct", "builder", "roofing"],
  photography:  ["photo", "imaging", "studio"],
  consulting:   ["consult", "advisory"],
  "pest control": ["pest", "exterminate"],
  general:      ["shop", "store", "service"],
};

// Patterns that indicate privacy-protected WHOIS — these are NOT real contacts
const PRIVACY_PATTERNS = [
  "privacy", "protect", "whoisguard", "contactprivacy", "domainsprivacy",
  "registrant@", "hidden", "redacted", "withheld", "noreply", "not disclosed",
];

function isPrivacyEmail(email: string | null | undefined): boolean {
  if (!email) return true;
  const lower = email.toLowerCase();
  return PRIVACY_PATTERNS.some((p) => lower.includes(p));
}

function guessCompanyName(domain: string): string {
  return domain
    .replace(/\.(com|net|org|io|co\.uk|co|biz|info|in|uk)$/i, "")
    .replace(/[-_.]/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .trim();
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 1: WhoisXML Newly Registered Domains
// ✅ FIX: correct endpoint — GET with apiKey + sinceDate + keywords[]
// Returns { domainsList: ["domain1.com", "domain2.com", ...] }
// ─────────────────────────────────────────────────────────────────────────────
async function fetchNewDomains(keyword: string, sinceDate: string): Promise<string[]> {
  if (!WHOISXML_KEY) {
    console.error("   [WhoisXML] ❌ No WHOISXML_KEY secret set");
    return [];
  }

  const body = {
    apiKey:       WHOISXML_KEY,
    sinceDate,
    domainType:   "new",
    outputFormat: "JSON",
    keywords:     [keyword],
  };

  console.log(`   [WhoisXML] POST ${WHOISXML_NEWREG} keyword="${keyword}"`);

  try {
    const res = await fetch(WHOISXML_NEWREG, {
      method:  "POST",
      headers: { "Content-Type": "application/json", "Accept": "application/json" },
      body:    JSON.stringify(body),
    });
    const text = await res.text();

    console.log(`   [WhoisXML] Status: ${res.status}`);
    console.log(`   [WhoisXML] Response: ${text.slice(0, 300)}`);

    if (res.status === 401) {
      console.error("   [WhoisXML] ❌ 401 — invalid API key");
      return [];
    }
    if (res.status === 402) {
      console.error("   [WhoisXML] ❌ 402 — credits exhausted. Check: user.whoisxmlapi.com/products");
      return [];
    }
    if (res.status === 429) {
      console.warn("   [WhoisXML] ⚠️  429 — rate limited, sleeping 10s");
      await new Promise((r) => setTimeout(r, 10_000));
      return [];
    }
    if (!res.ok) {
      console.error(`   [WhoisXML] ❌ ${res.status}: ${text.slice(0, 200)}`);
      return [];
    }

    let data: any;
    try { data = JSON.parse(text); }
    catch { console.error("   [WhoisXML] ❌ Could not parse JSON"); return []; }

    const list: string[] = data.domainsList || [];
    console.log(`   [WhoisXML] keyword="${keyword}" → ${list.length} domains returned`);

    if (list.length === 0) {
      console.warn(`   [WhoisXML] ⚠️  0 results. Possible causes:`);
      console.warn(`               1. DRS credits exhausted — check user.whoisxmlapi.com/products`);
      console.warn(`               2. sinceDate=${sinceDate} might have no registrations for this keyword`);
      console.warn(`               3. Keyword too narrow — try a shorter/broader term`);
    }

    return list.slice(0, MAX_DOMAINS_PER_RUN).map((d) => d.toLowerCase().trim());

  } catch (err) {
    console.error(`   [WhoisXML] ❌ Fetch error: ${(err as Error).message}`);
    return [];
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 2: websitelaunches.com — verify domain has NO website built
// Using single lookups (free tier doesn't include batch)
// ─────────────────────────────────────────────────────────────────────────────
async function checkUnbuilt(domain: string): Promise<{
  isUnbuilt:          boolean;
  domain_authority:   number;
  domain_age_days:    number;
  domain_category:    string | null;
  domain_subcategory: string | null;
  registered_date:    string | null;
}> {
  if (!WEBSITELAUNCHES_KEY) {
    // No key — assume unbuilt (less accurate but allows testing)
    console.warn(`   [WebsiteLaunches] ⚠️  No key — treating ${domain} as unbuilt (unverified)`);
    return { isUnbuilt: true, domain_authority: 0, domain_age_days: 7, domain_category: null, domain_subcategory: null, registered_date: null };
  }

  try {
    const res = await fetch(`${WEBL_BASE}/domain/${encodeURIComponent(domain)}`, {
      headers: { "X-API-Key": WEBSITELAUNCHES_KEY, "Content-Type": "application/json" },
    });

    if (res.status === 429) {
      console.warn(`   [WebsiteLaunches] ⚠️  429 — rate limit hit, skipping ${domain}`);
      return { isUnbuilt: false, domain_authority: 99, domain_age_days: 0, domain_category: null, domain_subcategory: null, registered_date: null };
    }
    if (!res.ok) {
      console.warn(`   [WebsiteLaunches] ⚠️  ${res.status} for ${domain}`);
      return { isUnbuilt: false, domain_authority: 99, domain_age_days: 0, domain_category: null, domain_subcategory: null, registered_date: null };
    }

    const data = await res.json() as any;
    const r    = data?.data || data;

    const authority = r?.site_authority ?? r?.domain_authority ?? 0;
    const launched  = r?.launch_detected === true;

    return {
      isUnbuilt:          !launched && authority <= MAX_AUTHORITY_SCORE,
      domain_authority:   authority,
      domain_age_days:    Math.floor((r?.domain_age ?? 0) * 365),
      domain_category:    r?.category    || null,
      domain_subcategory: r?.subcategory || null,
      registered_date:    r?.domain_age_date || null,
    };

  } catch (err) {
    console.warn(`   [WebsiteLaunches] ❌ Error for ${domain}: ${(err as Error).message}`);
    return { isUnbuilt: false, domain_authority: 99, domain_age_days: 0, domain_category: null, domain_subcategory: null, registered_date: null };
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 3: WhoisXML WHOIS lookup — get registrant contact from registration record
// ✅ FIX 2: Replaces Hunter.io (which cannot find emails on brand new domains)
// Same API key as Newly Registered Domains — uses "WHOIS credits" (separate pool)
// ~40–60% of domains will have real contact; rest use privacy protection
// ─────────────────────────────────────────────────────────────────────────────
async function whoisRegistrantLookup(domain: string): Promise<{
  email: string | null;
  name:  string | null;
  phone: string | null;
  org:   string | null;
}> {
  const EMPTY = { email: null, name: null, phone: null, org: null };
  if (!WHOISXML_KEY) return EMPTY;

  try {
    const url = `${WHOISXML_WHOIS}`
      + `?apiKey=${WHOISXML_KEY}`
      + `&domainName=${encodeURIComponent(domain)}`
      + `&outputFormat=JSON`;

    const res = await fetch(url);

    if (res.status === 402) {
      console.warn(`   [WHOIS] ⚠️  402 — WHOIS credits exhausted. Check user.whoisxmlapi.com/products`);
      return EMPTY;
    }
    if (!res.ok) {
      console.warn(`   [WHOIS] ⚠️  ${res.status} for ${domain}`);
      return EMPTY;
    }

    const data   = await res.json() as any;
    const record = data?.WhoisRecord;

    // Try registrant first, then technical contact as fallback
    const reg =
      record?.registrant         ||
      record?.technicalContact   ||
      record?.administrativeContact ||
      null;

    if (!reg) {
      console.log(`   [WHOIS] — No registrant record for ${domain}`);
      return EMPTY;
    }

    const raw_email = reg.email || null;

    // If email looks like a privacy service, discard it
    if (isPrivacyEmail(raw_email)) {
      console.log(`   [WHOIS] — Privacy-protected: ${domain} (${raw_email || "no email"})`);
      return EMPTY;
    }

    const result = {
      email: raw_email,
      name:  reg.name || (reg.firstName ? `${reg.firstName || ""} ${reg.lastName || ""}`.trim() : null),
      phone: reg.telephone    || null,
      org:   reg.organization || null,
    };

    console.log(`   [WHOIS] ✅ ${domain} → email: ${result.email} | name: ${result.name || "—"}`);
    return result;

  } catch (err) {
    console.warn(`   [WHOIS] ❌ ${domain}: ${(err as Error).message}`);
    return EMPTY;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 4 (optional): Hunter.io — ONLY for domains 30+ days old
// Not used in this pipeline (new domains) but exported for use in other functions
// ─────────────────────────────────────────────────────────────────────────────
async function hunterSearch(domain: string): Promise<string | null> {
  if (!HUNTER_KEY) return null;
  try {
    const res = await fetch(
      `https://api.hunter.io/v2/domain-search?domain=${domain}&api_key=${HUNTER_KEY}&limit=1`
    );
    if (!res.ok) return null;
    const data  = await res.json() as any;
    const first = data?.data?.emails?.[0];
    return first?.value || null;
  } catch { return null; }
}

// ═════════════════════════════════════════════════════════════════════════════
// MAIN HANDLER
// ═════════════════════════════════════════════════════════════════════════════
serve(async (req) => {
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    const {
      user_id,
      niche        = "restaurant",
      location     = "London",
      target_count = 33,
    } = await req.json();

    if (!user_id) throw new Error("Missing user_id");

    const sinceDate = new Date(Date.now() - MAX_DOMAIN_AGE_DAYS * 86_400_000)
      .toISOString().split("T")[0];

    console.log(`\n${"═".repeat(60)}`);
    console.log(`🌐 [NEW-DOMAIN-SCANNER]`);
    console.log(`   user_id:    ${user_id}`);
    console.log(`   niche:      ${niche}`);
    console.log(`   location:   ${location}`);
    console.log(`   target:     ${target_count}`);
    console.log(`   since_date: ${sinceDate}`);
    console.log(`   CREDIT PLAN:`);
    console.log(`     WhoisXML NRD:  ${MAX_DOMAINS_PER_RUN} domains`);
    console.log(`     WebsiteLaunch: ${MAX_WEBL_PER_RUN} lookups`);
    console.log(`     WHOIS lookup:  ${MAX_WHOIS_LOOKUPS} (registrant contact)`);
    console.log(`     Hunter.io:     SKIPPED (new domains have no indexed emails)`);
    console.log(`${"═".repeat(60)}`);

    // ── STEP 1: Fetch newly registered domains from WhoisXML ──────────────────
    console.log(`\n   Step 1: WhoisXML Newly Registered Domains…`);

    const keywords = NICHE_KEYWORDS[niche] || [niche.split(" ")[0].slice(0, 8)];
    console.log(`   Keywords: ${keywords.join(", ")}`);

    const allDomains = new Set<string>();

    for (const kw of keywords.slice(0, 2)) { // max 2 keywords per run to save credits
      const found = await fetchNewDomains(kw, sinceDate);
      found.forEach((d) => allDomains.add(d));
      if (allDomains.size >= MAX_DOMAINS_PER_RUN) break;
      await new Promise((r) => setTimeout(r, 400));
    }

    // Fallback: if 0 results with specific keywords, try first word only
    if (allDomains.size === 0 && keywords[0].length > 4) {
      console.log(`   ⚠️  0 results — trying broader keyword: "${keywords[0].slice(0, 4)}"`);
      const fallback = await fetchNewDomains(keywords[0].slice(0, 4), sinceDate);
      fallback.forEach((d) => allDomains.add(d));
    }

    const domainList = [...allDomains].slice(0, MAX_DOMAINS_PER_RUN);
    console.log(`\n   Total unique domains: ${domainList.length}`);

    if (domainList.length === 0) {
      console.log("   ⚠️  No domains found from WhoisXML. Possible issues:");
      console.log("       1. DRS credit balance = 0 (check user.whoisxmlapi.com/products)");
      console.log("       2. Keywords produced no matches for this date range");
      return new Response(
        JSON.stringify({ success: true, saved_count: 0, message: "No domains from WhoisXML" }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    // ── STEP 2: websitelaunches.com — verify unbuilt ──────────────────────────
    console.log(`\n   Step 2: Checking ${Math.min(domainList.length, MAX_WEBL_PER_RUN)} domains via websitelaunches.com…`);

    const unbuiltDomains: {
      domain: string;
      domain_authority:   number;
      domain_age_days:    number;
      domain_category:    string | null;
      domain_subcategory: string | null;
      registered_date:    string | null;
    }[] = [];

    for (const domain of domainList.slice(0, MAX_WEBL_PER_RUN)) {
      const result = await checkUnbuilt(domain);
      if (result.isUnbuilt) {
        unbuiltDomains.push({
          domain,
          domain_authority:   result.domain_authority,
          domain_age_days:    result.domain_age_days,
          domain_category:    result.domain_category,
          domain_subcategory: result.domain_subcategory,
          registered_date:    result.registered_date,
        });
        console.log(`   ✅ UNBUILT: ${domain} (authority=${result.domain_authority}, category=${result.domain_category || "—"})`);
      } else {
        console.log(`   ⏭️  SKIP: ${domain} (already launched or authority too high)`);
      }
      await new Promise((r) => setTimeout(r, 300)); // rate limit buffer
    }

    console.log(`\n   Unbuilt confirmed: ${unbuiltDomains.length} / ${Math.min(domainList.length, MAX_WEBL_PER_RUN)}`);

    if (unbuiltDomains.length === 0) {
      return new Response(
        JSON.stringify({ success: true, saved_count: 0, message: "All checked domains already launched" }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    // ── STEP 3: WHOIS registrant lookup (top N to save credits) ──────────────
    // ✅ FIX 2: WHOIS instead of Hunter — works on day-1 domains
    console.log(`\n   Step 3: WHOIS registrant lookup (top ${MAX_WHOIS_LOOKUPS} domains)…`);

    const contactMap = new Map<string, { email: string | null; name: string | null; phone: string | null; org: string | null }>();

    for (const d of unbuiltDomains.slice(0, MAX_WHOIS_LOOKUPS)) {
      const contact = await whoisRegistrantLookup(d.domain);
      contactMap.set(d.domain, contact);
      await new Promise((r) => setTimeout(r, 500)); // 1 req/sec
    }

    const enrichedCount = [...contactMap.values()].filter((c) => c.email).length;
    console.log(`\n   WHOIS results: ${enrichedCount} / ${contactMap.size} have real email`);

    // ── STEP 4: Build lead rows ───────────────────────────────────────────────
    console.log(`\n   Step 4: Building lead rows…`);

    const leadRows:    Record<string, unknown>[] = [];
    const seenDomains = new Set<string>();

    for (const d of unbuiltDomains) {
      if (leadRows.length >= target_count) break;
      if (seenDomains.has(d.domain)) continue;
      seenDomains.add(d.domain);

      const contact     = contactMap.get(d.domain) || { email: null, name: null, phone: null, org: null };
      const companyName = contact.org || guessCompanyName(d.domain);
      const email       = contact.email;
      const phone       = contact.phone;
      const contactName = contact.name;

      // Tier classification
      let tier = "tier_4_domain_only", tierLabel = "Domain only";
      if (email && phone)  { tier = "tier_1_verified";  tierLabel = "Email + phone found";  }
      else if (email)      { tier = "tier_2_email";     tierLabel = "Email found";           }
      else if (phone)      { tier = "tier_2_phone";     tierLabel = "Phone found";           }

      console.log(`\n   📌 ${d.domain}`);
      console.log(`      company:  ${companyName}`);
      console.log(`      email:    ${email    || "none"}`);
      console.log(`      phone:    ${phone    || "none"}`);
      console.log(`      name:     ${contactName || "none"}`);
      console.log(`      category: ${d.domain_category || "—"}`);
      console.log(`      age:      ${d.domain_age_days} days`);
      console.log(`      tier:     ${tier}`);

      leadRows.push({
        domain:        d.domain,
        company_name:  companyName,
        phone,
        email,
        address:       location,
        listing_url:   `https://${d.domain}`,
        place_id:      null,
        rating:        null,
        search_query:  `${niche} new domains`,
        source:        "new_domain",
        drip_source:   "cold_drip",
        has_website:   false,
        status:        "pending",

        // domain intelligence columns
        domain_launched:        false,
        domain_age_days:        d.domain_age_days,
        domain_authority:       d.domain_authority,
        domain_category:        d.domain_category,
        domain_subcategory:     d.domain_subcategory,
        domain_registered_date: d.registered_date || sinceDate,

        lead_data: {
          company_name:     companyName,
          contact_name:     contactName,
          domain:           d.domain,
          email,
          phone,
          website:          null,
          domain_age_days:  d.domain_age_days,
          domain_authority: d.domain_authority,
          domain_category:  d.domain_category,
          registered_date:  d.registered_date || sinceDate,
          has_website:      false,
          domain_launched:  false,
          signal:           "domain_bought_not_built",
          niche,
          location,
          tier,
          tier_label:       tierLabel,
          approach:         "new_domain",
          contact_strategy: email
            ? ["1. Email directly via registered email address"]
            : phone
            ? ["1. Call registered phone number"]
            : [
                "1. Visit the parked domain page for contact clues",
                "2. Check WHOIS at whois.domaintools.com",
                "3. Search Google: company name + city",
                "4. Search LinkedIn: company name",
              ],
          data_sources: {
            domain_list:  "WhoisXML Newly Registered Domains API",
            launch_check: WEBSITELAUNCHES_KEY ? "websitelaunches.com" : "skipped (no key)",
            contact:      email ? "WhoisXML WHOIS registrant" : "not found (privacy protected)",
          },
        },
      });
    }

    // ── STEP 5: Upsert ────────────────────────────────────────────────────────
    const { data: inserted, error: leadsErr } = await supabase
      .from("leads")
      .upsert(leadRows, { onConflict: "domain" })
      .select("id");

    if (leadsErr) throw new Error(`leads upsert: ${leadsErr.message}`);
    console.log(`\n   ✅ ${inserted?.length || 0} leads saved`);

    // ── STEP 6: Link to user_leads ────────────────────────────────────────────
    if (inserted?.length) {
      const { error: jErr } = await supabase
        .from("user_leads")
        .upsert(
          inserted.map((l: { id: string }) => ({ user_id, lead_id: l.id })),
          { onConflict: "user_id,lead_id" }
        );
      if (jErr) console.error(`   ❌ user_leads: ${jErr.message}`);
    }

    // ── STEP 7: Update monthly counter ───────────────────────────────────────
    const { data: profile } = await supabase
      .from("profiles").select("drip_leads_this_month").eq("id", user_id).single();
    if (profile) {
      await supabase.from("profiles")
        .update({ drip_leads_this_month: (profile.drip_leads_this_month || 0) + (inserted?.length || 0) })
        .eq("id", user_id);
    }

    console.log(`\n${"═".repeat(60)}`);
    console.log(`🏁 [NEW-DOMAIN-SCANNER] Done`);
    console.log(`   Domains from WhoisXML: ${domainList.length}`);
    console.log(`   Unbuilt confirmed:     ${unbuiltDomains.length}`);
    console.log(`   WHOIS enriched:        ${enrichedCount}`);
    console.log(`   Saved as leads:        ${inserted?.length || 0}`);
    console.log(`${"═".repeat(60)}\n`);

    return new Response(
      JSON.stringify({
        success:         true,
        domains_found:   domainList.length,
        unbuilt_count:   unbuiltDomains.length,
        enriched_count:  enrichedCount,
        saved_count:     inserted?.length || 0,
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );

  } catch (error) {
    const msg = (error as Error).message;
    console.error(`⛔ [NEW-DOMAIN-SCANNER CRASH] ${msg}`);
    return new Response(
      JSON.stringify({ error: msg }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
});