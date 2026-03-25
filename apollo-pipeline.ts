import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const APOLLO_API_KEY     = Deno.env.get("APOLLO_API_KEY")!;

const APOLLO_BASE       = "https://api.apollo.io/api/v1";
const MAX_COMPANIES     = 100;
const BATCH_SIZE        = 10;
const RATE_LIMIT_MS     = 200;
const DOMAIN_BATCH_SIZE = 25;

// ── 1. DOMAIN CLEANER ─────────────────────────────────────────────────────────
function canonicalDomain(raw: string | null | undefined): string | null {
  if (!raw || typeof raw !== "string") return null;
  const trimmed = raw.trim();
  if (!trimmed) return null;
  const withProtocol = trimmed.startsWith("http") ? trimmed : `https://${trimmed}`;
  try {
    let hostname = new URL(withProtocol).hostname.toLowerCase();
    hostname = hostname.replace(/^www\d*\./, "");
    if (hostname && hostname.includes(".")) return hostname;
  } catch { /* fall through */ }
  const bareMatch = trimmed.toLowerCase().replace(/^www\d*\./, "").match(/^([a-z0-9-]+\.)+[a-z]{2,}$/);
  if (bareMatch) return bareMatch[0];
  return null;
}

// ── 2. APOLLO FETCHER ─────────────────────────────────────────────────────────
async function apolloPost(
  path: string,
  body: Record<string, unknown>,
  retries = 3
): Promise<Record<string, unknown>> {
  for (let attempt = 0; attempt < retries; attempt++) {
    const res = await fetch(`${APOLLO_BASE}${path}`, {
      method:  "POST",
      headers: { "Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY },
      body:    JSON.stringify(body),
    });
    if (res.status === 429) {
      const wait = Math.pow(2, attempt) * 1500;
      console.warn(`   [Apollo] Rate limited. Waiting ${wait}ms…`);
      await new Promise((r) => setTimeout(r, wait));
      continue;
    }
    if (!res.ok) throw new Error(`Apollo ${path} → ${res.status}: ${await res.text()}`);
    return res.json();
  }
  throw new Error(`Apollo ${path} → max retries exceeded`);
}

// ── 3. TIER CLASSIFICATION ────────────────────────────────────────────────────
function classifyTier(emailStatus: string | undefined): { tier: string; tierLabel: string } {
  if (emailStatus === "verified")         return { tier: "tier_1_verified",      tierLabel: "Verified email"   };
  if (emailStatus === "likely_to_engage") return { tier: "tier_2_email",         tierLabel: "Likely email"     };
  if (emailStatus && emailStatus !== "invalid" && emailStatus !== "do_not_email")
                                          return { tier: "tier_2_email",         tierLabel: "Unverified email" };
  return                                  { tier: "tier_3_linkedin_only", tierLabel: "LinkedIn only"    };
}

// ── 4. BUILD ORG SEARCH FILTERS ───────────────────────────────────────────────
const FILLER_WORDS    = new Set(["in","at","near","based","from","the","of","and","for","with","a","an"]);
const KNOWN_LOCATIONS = new Set([
  "india","usa","us","uk","canada","australia","germany","france","singapore","uae",
  "dubai","london","berlin","paris","sydney","toronto","chicago","texas","california",
  "delhi","mumbai","bangalore","hyderabad","pune","chennai","kolkata","ahmedabad",
  "japan","korea","china","brazil","mexico","spain","italy","netherlands","sweden",
  "new york","new","york","angeles","los","san","francisco",
]);

function buildFilters(search_query: string): Record<string, unknown> {
  const words = search_query.trim().split(/\s+/);
  const locationTags: string[] = [];
  const keywordTags:  string[] = [];

  for (const word of words) {
    const lower = word.toLowerCase().replace(/[^a-z]/g, "");
    if (!lower) continue;
    if (FILLER_WORDS.has(lower)) continue;
    if (KNOWN_LOCATIONS.has(lower)) locationTags.push(word.trim());
    else keywordTags.push(word.trim());
  }

  if (keywordTags.length === 0) keywordTags.push(search_query.trim());

  console.log(`   [buildFilters] keyword_tags: ${JSON.stringify(keywordTags)}`);
  console.log(`   [buildFilters] locations:    ${JSON.stringify(locationTags)}`);

  const filters: Record<string, unknown> = {
    q_organization_keyword_tags: keywordTags,
    per_page: MAX_COMPANIES,
    page: 1,
  };
  if (locationTags.length > 0) filters.organization_locations = locationTags;
  return filters;
}

// ── 5. PEOPLE SEARCH — returns people mapped by DOMAIN ───────────────────────
async function fetchDecisionMakersByDomain(
  domains: string[]
): Promise<Map<string, any>> {
  const personByDomain = new Map<string, any>();

  for (let i = 0; i < domains.length; i += DOMAIN_BATCH_SIZE) {
    const batch = domains.slice(i, i + DOMAIN_BATCH_SIZE);
    try {
      // ✅ FIX 1: We must use api_search to avoid the 422 error
      const data = await apolloPost("/mixed_people/api_search", {
        q_organization_domains_list: batch,
        person_titles:      ["CEO", "Founder", "Co-Founder", "Managing Director", "President", "Owner", "Director"],
        person_seniorities: ["owner", "founder", "c_suite"],
        per_page: 50,
        page: 1,
      }) as any;

      const found: any[] = data.people || [];
      console.log(`   People batch ${i}–${i + DOMAIN_BATCH_SIZE}: ${found.length} found`);

      for (const p of found) {
        // Fallbacks for extracting the domain since api_search data structures vary
        const personDomain = canonicalDomain(
          p.organization?.primary_domain ||
          p.organization?.website_url    ||
          p.organization_domain          ||
          (p.email ? p.email.split("@")[1] : null)
        );

        if (!personDomain) continue;

        // ✅ FIX 2: Since api_search drops linkedin_url, we score purely on email and phone flags
        if (!personByDomain.has(personDomain)) {
          personByDomain.set(personDomain, p);
        } else {
          const existing = personByDomain.get(personDomain);
          const existingScore = (existing.has_email ? 2 : 0) + (existing.has_direct_phone === "Yes" ? 1 : 0);
          const newScore      = (p.has_email        ? 2 : 0) + (p.has_direct_phone === "Yes"        ? 1 : 0);
          if (newScore > existingScore) personByDomain.set(personDomain, p);
        }
      }
    } catch (e) {
      console.warn(`   People batch ${i}–${i + DOMAIN_BATCH_SIZE} failed: ${(e as Error).message}`);
    }

    if (i + DOMAIN_BATCH_SIZE < domains.length) {
      await new Promise((r) => setTimeout(r, RATE_LIMIT_MS));
    }
  }

  console.log(`   Mapped ${personByDomain.size} unique contacts by domain`);
  return personByDomain;
}

// ── 6. BULK ENRICH — strictly by ID ───────────────────────────────────────────
async function enrichContacts(
  people: any[]
): Promise<Map<string, any>> {
  const enrichedById = new Map<string, any>(); 

  // ✅ FIX 3: api_search only gives us Apollo ID, so we enrich strictly by ID
  const validPeople = people.filter((p) => p.id);
  console.log(`   Enriching ${validPeople.length} via Apollo ID`);

  for (let i = 0; i < validPeople.length; i += BATCH_SIZE) {
    const batch = validPeople.slice(i, i + BATCH_SIZE);
    try {
      const enrichData = await apolloPost("/people/bulk_match", {
        details: batch.map((p) => ({ id: p.id })),
        reveal_personal_emails: false,
      }) as any;

      for (const m of (enrichData.matches || [])) {
        if (m.id) enrichedById.set(m.id, m);
      }
    } catch (e) {
      console.warn(`   ID enrich batch ${i} failed: ${(e as Error).message}`);
    }
    if (i + BATCH_SIZE < validPeople.length) {
      await new Promise((r) => setTimeout(r, RATE_LIMIT_MS));
    }
  }

  console.log(`   ✅ Enriched ${enrichedById.size} contacts total`);
  return enrichedById;
}

// ─── helper: find the enriched record for a given person ─────────────────────
function findEnriched(
  person: any,
  enrichedMap: Map<string, any>
): any | undefined {
  return person.id ? enrichedMap.get(person.id) : undefined;
}

// ═════════════════════════════════════════════════════════════════════════════
// MAIN EDGE FUNCTION
// ═════════════════════════════════════════════════════════════════════════════
serve(async (req) => {
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    const { pref_id, user_id, search_query } = await req.json();
    if (!pref_id || !user_id || !search_query) {
      throw new Error("Missing required fields: pref_id, user_id, search_query");
    }

    console.log(`\n🔵 [APOLLO-PIPELINE] Job: ${pref_id} | Query: "${search_query}"`);

    // ── STEP 1: Search companies ──────────────────────────────────────────────
    console.log("   Step 1: Searching companies…");
    const orgData = await apolloPost("/mixed_companies/search", buildFilters(search_query)) as any;
    const orgs: any[] = orgData.organizations || [];
    console.log(`   Found ${orgs.length} companies`);

    if (orgs.length === 0) {
      await supabase.from("lead_preferences")
        .update({ status: "completed", completed_at: new Date().toISOString() })
        .eq("id", pref_id);
      return new Response(
        JSON.stringify({ success: true, companies_saved: 0, message: "No companies found" }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    // ── STEP 2: Find decision-makers — matched by DOMAIN ─────────────────────
    console.log("   Step 2: Finding decision-makers by domain…");
    const orgDomains = orgs
      .map((o) => canonicalDomain(o.website_url || o.primary_domain))
      .filter(Boolean) as string[];

    console.log(`   Searching people for ${orgDomains.length} domains`);

    const personByDomain = orgDomains.length > 0
      ? await fetchDecisionMakersByDomain(orgDomains)
      : new Map<string, any>();

    console.log(`   Found ${personByDomain.size} / ${orgs.length} companies have a matched contact`);

    // ── STEP 3: Bulk-enrich via Apollo ID ─────────────────────────────────────
    console.log("   Step 3: Enriching contacts…");
    const allPeople = [...personByDomain.values()];
    const enrichedMap = allPeople.length > 0
      ? await enrichContacts(allPeople)
      : new Map<string, any>();

    // ── STEP 4: Build rows ────────────────────────────────────────────────────
    const rows: Record<string, unknown>[] = [];
    const seenDomains = new Set<string>();

    let matchedCount   = 0;
    let enrichedCount  = 0;

    for (const org of orgs) {
      const domain = canonicalDomain(org.website_url || org.primary_domain);
      if (!domain || seenDomains.has(domain)) continue;
      seenDomains.add(domain);

      const person   = personByDomain.get(domain);
      const enriched = person ? findEnriched(person, enrichedMap) : undefined;

      if (person)   matchedCount++;
      if (enriched) enrichedCount++;

      const { tier, tierLabel } = classifyTier(enriched?.email_status);

      rows.push({
        domain,
        apollo_id:         org.id                                                || null,
        preference_id:     pref_id,
        search_query,
        source:            "apollo",

        // Company fields
        name:              org.name                                              || null,
        website_url:       org.website_url                                       || null,
        industry:          org.industry                                          || null,
        employee_count:    org.estimated_num_employees ?? org.num_employees      ?? null,
        estimated_rev:     org.annual_revenue_printed                            || null,
        founded_year:      org.founded_year                                      || null,
        hq_country:        org.country                                           || null,
        city:              org.city                                              || null,
        state:             org.state                                             || null,
        short_description: org.short_description                                 || null,
        linkedin_url:      org.linkedin_url                                      || null,
        twitter_url:       org.twitter_url                                       || null,
        phone:             org.phone ?? org.sanitized_phone                      ?? null,
        tech_stack:        (org.current_technologies || org.technologies || []).map(
                             (t: any) => ({ name: t.name, uid: t.uid })
                           ),

        // Contact fields
        contact_name:         enriched?.name         || person?.name         || person?.first_name || null,
        contact_title:        enriched?.title        || person?.title                                      || null,
        contact_email:        enriched?.email                                                              || null,
        contact_email_status: enriched?.email_status 
                              || (person?.has_email === true ? "unverified" : null),
        contact_phone:        enriched?.phone_numbers?.[0]?.raw_number                                     || null,
        contact_linkedin_url: enriched?.linkedin_url || person?.linkedin_url                               || null,

        tier,
        tier_label:       tierLabel,
        enriched:         !!enriched,
        enrichment_error: null,
        enriched_at:      enriched ? new Date().toISOString() : null,
        updated_at:       new Date().toISOString(),
      });
    }

    console.log(`   Built ${rows.length} rows: ${matchedCount} with contact, ${enrichedCount} with verified email`);

    // ── STEP 5: Upsert into companies table ───────────────────────────────────
    if (rows.length > 0) {
      const { error: upsertErr } = await supabase
        .from("companies")
        .upsert(rows, { onConflict: "domain" });
      if (upsertErr) throw new Error(`companies upsert failed: ${upsertErr.message}`);
      console.log(`   ✅ ${rows.length} companies saved`);
    }

    // ── STEP 6: Mark completed ────────────────────────────────────────────────
    await supabase.from("lead_preferences")
      .update({ status: "completed", completed_at: new Date().toISOString() })
      .eq("id", pref_id);

    console.log(`   ✅ Job ${pref_id} completed`);

    return new Response(
      JSON.stringify({
        success:         true,
        companies_saved: rows.length,
        matched_contacts: matchedCount,
        enriched_contacts: enrichedCount,
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );

  } catch (error) {
    const msg = (error as Error).message;
    console.error("⛔ [APOLLO-PIPELINE]", msg);
    try {
      const { pref_id } = await req.clone().json();
      if (pref_id) {
        await createClient(supabaseUrl, supabaseServiceKey)
          .from("lead_preferences")
          .update({ status: "failed", error_message: msg })
          .eq("id", pref_id);
      }
    } catch { /* ignore */ }
    return new Response(
      JSON.stringify({ error: msg }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
});
