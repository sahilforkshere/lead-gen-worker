// ═══════════════════════════════════════════════════════════════════════════════
// EDGE FUNCTION: new-domain-scanner (B2B / DIGITAL PIPELINE)
//
// FILE: supabase/functions/new-domain-scanner/index.ts
// DEPLOY: supabase functions deploy new-domain-scanner
//
// SECRETS NEEDED:
//   supabase secrets set WHOISXML_KEY=your_whoisxml_api_key
//   supabase secrets set WEBSITELAUNCHES_KEY=your_websitelaunches_key
//   supabase secrets set HUNTER_API_KEY=your_hunter_key
//
// ── CREDIT SPEND PER RUN (Target: 33 leads/night) ────────────────────────────
//   WhoisXML      : 1 credit per run (searches 4 keywords simultaneously)
//   WebsiteLaunch : 1 batch check per run (free tier allows 3,000/mo)
//   Hunter.io     : Max 5 calls per run (protects your 25/mo free limit)
// ═══════════════════════════════════════════════════════════════════════════════

import { serve }        from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const WHOISXML_KEY       = Deno.env.get("WHOISXML_KEY");
const WEBSITELAUNCHES_KEY= Deno.env.get("WEBSITELAUNCHES_KEY");
const HUNTER_KEY         = Deno.env.get("HUNTER_API_KEY");

// ── Credit guards & Thresholds ───────────────────────────────────────────────
const MAX_WHOISXML_RESULTS        = 100; // Cap the raw domains we process
const MAX_WEBSITELAUNCHES_PER_RUN = 100; // Batch size for launch checking
const MAX_HUNTER_PER_RUN          = 5;   // Hunter calls per run (protects free tier)
const MAX_DOMAIN_AGE_DAYS         = 14;  // Only fetch domains registered in the last 14 days
const MAX_AUTHORITY_SCORE         = 5;   // 0-5 = genuinely new, no web history

// ── API Base URLs ────────────────────────────────────────────────────────────
const WHOISXML_BASE = "https://domains-subdomains-discovery.whoisxmlapi.com/api/v1";
const WEBL_BASE     = "https://websitelaunches.com/api/v1";

// ── Keyword map — what to search in WhoisXML for each niche ──────────────────
const NICHE_KEYWORDS: Record<string, string[]> = {
  restaurant:   ["restaurant", "cafe", "bistro", "kitchen", "diner", "eatery"],
  plumber:      ["plumbing", "plumber", "pipes", "heating", "drain"],
  electrician:  ["electric", "electrician", "wiring", "solar", "power"],
  "pest control": ["pest", "exterminator", "bugs", "termite"],
  salon:        ["salon", "beauty", "hair", "spa", "nails", "barber"],
  dental:       ["dental", "dentist", "teeth", "orthodontic", "smile"],
  gym:          ["gym", "fitness", "yoga", "pilates", "crossfit", "training"],
  lawyer:       ["law", "legal", "attorney", "solicitor", "advocate"],
  accountant:   ["accounting", "accountant", "bookkeeping", "tax", "finance"],
  realEstate:   ["realty", "realtor", "properties", "estate", "homes"],
  cleaning:     ["cleaning", "cleaner", "janitorial", "housekeeping", "maid"],
  construction: ["construction", "builder", "contractor", "renovation", "roofing"],
  photography:  ["photography", "photographer", "photo", "studio", "imaging"],
  consulting:   ["consulting", "consultancy", "advisory", "solutions", "services"],
  general:      ["shop", "store", "service", "local", "pro", "agency"],
};

// ─────────────────────────────────────────────────────────────────────────────
// STEP 1: WhoisXML Domains & Subdomains Discovery
// Method: POST
// ─────────────────────────────────────────────────────────────────────────────
async function fetchWhoisDomains(
  keywords: string[],
  sinceDate: string   // YYYY-MM-DD
): Promise<{ domain: string; createdAt: string }[]> {
  if (!WHOISXML_KEY) {
    console.error("   [WhoisXML] ❌ No API key set. Set WHOISXML_KEY secret.");
    return [];
  }

  console.log(`   [WhoisXML] POST Request initiated...`);
  console.log(`   [WhoisXML] Keywords: [${keywords.join(", ")}]`);
  console.log(`   [WhoisXML] Since Date: ${sinceDate}`);

  try {
    const res = await fetch(WHOISXML_BASE, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        apiKey: WHOISXML_KEY,
        sinceDate: sinceDate,
        domains: { include: keywords }
      }),
    });

    if (res.status === 401 || res.status === 403) {
      console.error(`   [WhoisXML] ❌ Auth Error (${res.status}) — check your WHOISXML_KEY or credits`);
      return [];
    }
    if (!res.ok) {
      const body = await res.text();
      console.error(`   [WhoisXML] ❌ ${res.status}: ${body.slice(0, 200)}`);
      return [];
    }

    const data = await res.json() as any;
    
    // WhoisXML Discovery API returns an array of strings in `domainsList`
    const raw: string[] = data.domainsList || [];

    if (!Array.isArray(raw)) {
      console.error("   [WhoisXML] ❌ Unexpected response shape:", JSON.stringify(data).slice(0, 200));
      return [];
    }

    // Format the domains and apply a strict limit to protect downstream APIs
    const recent = raw
      .filter((d) => d.trim().length > 0)
      .slice(0, MAX_WHOISXML_RESULTS)
      .map((domain) => ({
        domain: domain.toLowerCase().trim(),
        createdAt: sinceDate, 
      }));

    console.log(`   [WhoisXML] ✅ Success! Found ${raw.length} total domains matching criteria.`);
    console.log(`   [WhoisXML] Pushing top ${recent.length} domains to the next pipeline stage.`);

    return recent;

  } catch (err) {
    console.error(`   [WhoisXML] ❌ Fetch error: ${(err as Error).message}`);
    return [];
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 2: websitelaunches.com BATCH
// Returns only domains where launch_detected = false AND authority <= threshold
// ─────────────────────────────────────────────────────────────────────────────
async function checkUnbuiltDomains(domains: string[]): Promise<{
  domain:             string;
  domain_authority:   number;
  domain_age_days:    number;
  domain_category:    string | null;
  domain_subcategory: string | null;
  registered_date:    string | null;
}[]> {
  if (!WEBSITELAUNCHES_KEY) {
    console.warn("   [WebsiteLaunches] ⚠️ No API key — treating all as unbuilt for testing.");
    return domains.map((d) => ({
      domain: d, domain_authority: 0, domain_age_days: 7,
      domain_category: null, domain_subcategory: null, registered_date: null,
    }));
  }

  const batch = domains.slice(0, MAX_WEBSITELAUNCHES_PER_RUN);
  console.log(`   [WebsiteLaunches] Batch checking ${batch.length} domains…`);

  try {
    const res = await fetch(`${WEBL_BASE}/domain/batch`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-API-Key":    WEBSITELAUNCHES_KEY,
      },
      body: JSON.stringify({ domains: batch }),
    });

    if (res.status === 401) {
      console.error("   [WebsiteLaunches] ❌ 401 — check WEBSITELAUNCHES_KEY");
      return [];
    }
    if (res.status === 429) {
      console.warn("   [WebsiteLaunches] ⚠️ 429 Rate limited — month credits exhausted");
      return [];
    }
    if (!res.ok) {
      console.error(`   [WebsiteLaunches] ❌ ${res.status}: ${await res.text()}`);
      return [];
    }

    const data    = await res.json() as any;
    const results = data?.data?.results || [];

    if (!Array.isArray(results)) return [];

    // Filter: no website ever built + authority is very low
    const unbuilt = results.filter((r: any) =>
      r.launch_detected === false && (r.domain_authority ?? 0) <= MAX_AUTHORITY_SCORE
    );

    console.log(`   [WebsiteLaunches] ✅ ${results.length} checked → ${unbuilt.length} confirmed strictly unbuilt!`);

    return unbuilt.map((r: any) => ({
      domain:             (r.domain || "").toLowerCase(),
      domain_authority:   r.domain_authority    ?? 0,
      domain_age_days:    Math.floor((r.domain_age ?? 0) * 365),
      domain_category:    r.category            || null,
      domain_subcategory: r.subcategory         || null,
      registered_date:    r.domain_age_date     || null,
    }));

  } catch (err) {
    console.error(`   [WebsiteLaunches] ❌ Fetch error: ${(err as Error).message}`);
    return [];
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// STEP 3: Hunter.io domain search — email enrichment
// ─────────────────────────────────────────────────────────────────────────────
async function hunterDomainSearch(domain: string): Promise<{
  email:      string | null;
  name:       string | null;
  confidence: number;
}> {
  if (!HUNTER_KEY) {
    console.warn(`   [Hunter] ⚠️ No HUNTER_API_KEY set — skipping enrichment`);
    return { email: null, name: null, confidence: 0 };
  }

  try {
    const url = `https://api.hunter.io/v2/domain-search?domain=${encodeURIComponent(domain)}&api_key=${HUNTER_KEY}&limit=1`;
    console.log(`   [Hunter] Searching: ${domain}`);

    const res = await fetch(url);

    if (res.status === 401) return { email: null, name: null, confidence: 0 };
    if (res.status === 429) {
      console.warn("   [Hunter] ⚠️ 429 — monthly free limit hit");
      return { email: null, name: null, confidence: 0 };
    }
    if (res.status === 404 || !res.ok) return { email: null, name: null, confidence: 0 };

    const data   = await res.json() as any;
    const emails = data?.data?.emails || [];

    if (emails.length === 0) return { email: null, name: null, confidence: 0 };

    // Pick highest-confidence email
    const best = emails.sort((a: any, b: any) => (b.confidence ?? 0) - (a.confidence ?? 0))[0];
    const found = {
      email:      best.value      || null,
      name:       best.first_name ? `${best.first_name} ${best.last_name || ""}`.trim() : null,
      confidence: best.confidence ?? 0,
    };

    console.log(`   [Hunter] ✅ Found: ${found.email} (confidence: ${found.confidence}%)`);
    return found;

  } catch (err) {
    console.error(`   [Hunter] ❌ Fetch error: ${(err as Error).message}`);
    return { email: null, name: null, confidence: 0 };
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Guess a company name from the domain ("sun-cafe-london.com" → "Sun Cafe London")
// ─────────────────────────────────────────────────────────────────────────────
function guessCompanyName(domain: string): string {
  return domain
    .replace(/\.(com|net|org|io|co\.uk|co|biz|info|in)$/i, "")
    .replace(/[-_.]/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .trim();
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

    const today = new Date();
    // Calculate the exact boundary date for "new" domains (14 days ago)
    const sinceDate = new Date(today.getTime() - MAX_DOMAIN_AGE_DAYS * 86_400_000)
      .toISOString().split("T")[0]; // Format: YYYY-MM-DD

    console.log(`\n${"═".repeat(60)}`);
    console.log(`🌐 [NEW-DOMAIN-SCANNER] Executing Pipeline`);
    console.log(`   User ID:    ${user_id}`);
    console.log(`   Niche:      ${niche}`);
    console.log(`   Location:   ${location}`);
    console.log(`   Target:     ${target_count} leads`);
    console.log(`   Boundary:   Registered since ${sinceDate}`);
    console.log(`${"═".repeat(60)}`);

    // ── STEP 1: Collect keywords for this niche ─────────────────────────────
    // We slice to 4 keywords max because WhoisXML allows up to 4 per API request
    const keywordsToSearch = (NICHE_KEYWORDS[niche] || NICHE_KEYWORDS["general"]).slice(0, 4);
    
    console.log(`\n   Step 1: WhoisXML Discovery API...`);
    const rawDomains = await fetchWhoisDomains(keywordsToSearch, sinceDate);

    if (rawDomains.length === 0) {
      console.log("   ⚠️  No new domains found. Terminating pipeline early.");
      return new Response(
        JSON.stringify({ success: true, saved_count: 0, message: "No domains found from WhoisXML" }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    // ── STEP 2: websitelaunches.com batch — filter to unbuilt only ──────────
    console.log(`\n   Step 2: WebsiteLaunches API (Launch Verification)...`);
    // Extract just the string domains from the raw objects
    const domainStringList = rawDomains.map(d => d.domain);
    const unbuilt = await checkUnbuiltDomains(domainStringList);

    if (unbuilt.length === 0) {
      console.log("   ⚠️  All checked domains already have websites built. Nothing to save.");
      return new Response(
        JSON.stringify({ success: true, saved_count: 0, message: "All domains already launched" }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    // ── STEP 3: Hunter enrichment — TOP N only to guard free credits ─────────
    // Sort by domain age (newest domains first = highest intent)
    const sorted = [...unbuilt].sort((a, b) => a.domain_age_days - b.domain_age_days);
    
    // We slice the array so we only spend Hunter credits on the absolute best leads
    const toEnrich = sorted.slice(0, MAX_HUNTER_PER_RUN);
    const skipEnrich = sorted.slice(MAX_HUNTER_PER_RUN);

    console.log(`\n   Step 3: Hunter.io Enrichment...`);
    console.log(`   Enriching top ${toEnrich.length} leads (Skipping ${skipEnrich.length} to preserve API credits)`);

    const emailMap = new Map<string, { email: string | null; name: string | null }>();

    for (const d of toEnrich) {
      const result = await hunterDomainSearch(d.domain);
      emailMap.set(d.domain, { email: result.email, name: result.name });
      await new Promise((r) => setTimeout(r, 600)); // 1 req/sec to avoid Hunter rate limits
    }

    // ── STEP 4: Build lead rows ──────────────────────────────────────────────
    console.log(`\n   Step 4: Building Database Rows…`);

    const leadRows:    Record<string, unknown>[] = [];
    const seenDomains = new Set<string>();

    for (const d of sorted) {
      if (leadRows.length >= target_count) break;
      if (seenDomains.has(d.domain)) continue;
      seenDomains.add(d.domain);

      const enrich      = emailMap.get(d.domain);
      const email       = enrich?.email || null;
      const contactName = enrich?.name  || null;
      const companyName = guessCompanyName(d.domain);
      
      // Tier based on data available (Important for your frontend UI Badges!)
      let tier      = "tier_4_domain_only";
      let tierLabel = "Domain only — no website built";
      if (email) { 
          tier = "tier_1_enriched"; 
          tierLabel = "Enriched Email Found"; 
      }

      console.log(`   ✅ Processed: ${d.domain} | Category: ${d.domain_category || "N/A"} | Email: ${email ? "YES" : "NO"}`);

      leadRows.push({
        // 🚀 Top-level leads columns (Perfectly aligned with cold-leads-drip)
        domain:       d.domain,
        company_name: companyName,
        phone:        null,            // Explicit nulls keep UI clean
        email:        email,
        address:      location,
        listing_url:  `https://${d.domain}`,
        place_id:     null,            // Maps API specific, so null here
        rating:       null,            // Maps API specific, so null here

        search_query: `${niche} new domains`,
        source:       "new_domain",
        drip_source:  "cold_drip",
        has_website:  false,
        status:       "pending",
        
        // Domain intelligence columns
        domain_launched:        false,
        domain_age_days:        d.domain_age_days,
        domain_authority:       d.domain_authority,
        domain_category:        d.domain_category,
        domain_subcategory:     d.domain_subcategory,
        domain_registered_date: d.registered_date || sinceDate, // Fallback to boundary

        // Full JSONB metadata
        lead_data: {
          company_name:     companyName,
          contact_name:     contactName,
          domain:           d.domain,
          email:            email,
          phone:            null,
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
            ? ["1. Email directly using found email address"]
            : [
                "1. Check WHOIS registrant email at whois.domaintools.com",
                "2. Search Google for company name + city",
                "3. Search LinkedIn for company name",
              ],
          data_sources: {
            domains:      "WhoisXML Domains Discovery API",
            launch_check: "WebsiteLaunches.com",
            email:        email ? "Hunter.io" : "Not Enriched",
          },
        },
      });
    }

    console.log(`\n   Pipeline Summary:`);
    console.log(`   Domains from WhoisXML:    ${rawDomains.length}`);
    console.log(`   Confirmed unbuilt:        ${unbuilt.length}`);
    console.log(`   Hunter enriched:          ${[...emailMap.values()].filter(e => e.email).length}`);
    console.log(`   Leads ready to save:      ${leadRows.length}`);

    if (leadRows.length === 0) {
      return new Response(
        JSON.stringify({ success: true, saved_count: 0, message: "No leads to save after filtering" }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    // ── STEP 5: Upsert to leads ──────────────────────────────────────────────
    console.log(`\n   Step 5: Writing to Supabase Leads Table...`);
    const { data: inserted, error: leadsErr } = await supabase
      .from("leads")
      .upsert(leadRows, { onConflict: "domain" })
      .select("id");

    if (leadsErr) {
      console.error(`   ❌ leads upsert error: ${leadsErr.message}`);
      throw new Error(`leads upsert: ${leadsErr.message}`);
    }

    console.log(`   ✅ ${inserted?.length || 0} unique leads successfully upserted.`);

    // ── STEP 6: Link to user_leads ───────────────────────────────────────────
    if (inserted && inserted.length > 0) {
      const { error: jErr } = await supabase
        .from("user_leads")
        .upsert(
          inserted.map((l: { id: string }) => ({ user_id, lead_id: l.id })),
          { onConflict: "user_id,lead_id" }
        );

      if (jErr) console.error(`   ❌ user_leads mapping error: ${jErr.message}`);
      else      console.log(`   ✅ user_leads mapping complete.`);
    }

    // ── STEP 7: Update monthly counter ──────────────────────────────────────
    const { data: profile, error: profErr } = await supabase
      .from("profiles")
      .select("drip_leads_this_month")
      .eq("id", user_id)
      .single();

    if (profErr) {
      console.warn(`   ⚠️ Could not fetch profile for counter update: ${profErr.message}`);
    } else if (profile) {
      await supabase
        .from("profiles")
        .update({ drip_leads_this_month: (profile.drip_leads_this_month || 0) + (inserted?.length || 0) })
        .eq("id", user_id);
      console.log(`   ✅ Monthly quota tracker updated.`);
    }

    console.log(`\n${"═".repeat(60)}`);
    console.log(`🏁 [NEW-DOMAIN-SCANNER] Pipeline Complete`);
    console.log(`   Saved: ${inserted?.length || 0}`);
    console.log(`${"═".repeat(60)}\n`);

    return new Response(
      JSON.stringify({
        success:         true,
        domains_checked: rawDomains.length,
        unbuilt_count:   unbuilt.length,
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