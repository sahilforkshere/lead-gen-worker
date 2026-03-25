import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const APOLLO_API_KEY     = Deno.env.get("APOLLO_API_KEY")!;

const APOLLO_BASE       = "https://api.apollo.io/api/v1";
const MAX_COMPANIES     = 5;   // <--- Now it only fetches 5
const BATCH_SIZE        = 10;
const RATE_LIMIT_MS     = 200;

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

// ── 2. HYPER-DETAILED APOLLO FETCHER ──────────────────────────────────────────
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
      console.warn(`   [Apollo] Rate limited on ${path}. Waiting ${wait}ms…`);
      await new Promise((r) => setTimeout(r, wait));
      continue;
    }
    
    if (!res.ok) {
      const errorText = await res.text();
      console.error(`\n❌ [APOLLO API ERROR] Request Failed!`);
      console.error(`👉 Endpoint: ${path}`);
      console.error(`👉 Payload: \n${JSON.stringify(body, null, 2)}`);
      throw new Error(`Apollo ${path} -> ${res.status}: ${errorText}`);
    }
    
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

// ── 4. SMART LOCATION EXTRACTOR & FILTER BUILDER ──────────────────────────────
function buildFilters(search_query: string): Record<string, unknown> {
  let keywords = search_query.trim();
  const locationTags: string[] = [];

  // ✅ SMART EXTRACTOR: If the user types "in Europe" or "in London", extract it!
  const inMatch = search_query.match(/\bin\s+([A-Za-z\s]+)$/i);
  if (inMatch) {
    locationTags.push(inMatch[1].trim()); // Pushes "Europe" to locations
    keywords = search_query.replace(inMatch[0], '').trim(); // Leaves "VC Firms"
  }

  // Filter out tiny filler words, keep real keywords (VC, Firms, SaaS, etc.)
  const keywordTags = keywords.split(/\s+/).filter(w => w.length > 1);
  if (keywordTags.length === 0) keywordTags.push(keywords);

  const filters: Record<string, unknown> = {
    q_organization_keyword_tags: keywordTags, 
    per_page: MAX_COMPANIES,
    page: 1,
  };
  
  if (locationTags.length > 0) {
    // Tells Apollo to STRICTLY limit search to this geography
    filters.organization_locations = locationTags; 
  }
  
  return filters;
}

// ═════════════════════════════════════════════════════════════════════════════
// MAIN EDGE FUNCTION
// ═════════════════════════════════════════════════════════════════════════════
serve(async (req) => {
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    const { pref_id, user_id, search_query } = await req.json();
    if (!pref_id || !user_id || !search_query) {
      throw new Error("Missing required fields");
    }

    console.log(`\n🔵 [APOLLO-PIPELINE] Job: ${pref_id} | Query: "${search_query}"`);

    // ── STEP 1: Search companies ──────────────────────────────────────────────
    const filters = buildFilters(search_query);
    console.log(`   Step 1: Searching companies with Locations: [${filters.organization_locations || "Global"}]`);
    
    const orgData = await apolloPost("/mixed_companies/search", filters) as any;
    const orgs: any[] = orgData.organizations || [];
    console.log(`   Found ${orgs.length} companies`);

    if (orgs.length === 0) {
      await supabase.from("lead_preferences").update({ status: "completed" }).eq("id", pref_id);
      return new Response(JSON.stringify({ success: true, count: 0 }));
    }

    // ── STEP 2: Find decision-makers ──────────────────────────────────────────
    console.log("   Step 2: Finding decision-makers…");
    const orgIds = orgs.map((o: any) => o.id).filter(Boolean);

    let people: any[] = [];
    if (orgIds.length > 0) {
      try {
        const peopleData = await apolloPost("/mixed_people/api_search", {
          organization_ids: orgIds, 
          person_titles: ["CEO", "Founder", "Partner", "Managing Director", "President", "Owner"],
          person_seniorities: ["owner", "founder", "c_suite", "partner"],
          per_page: 100,
          page: 1,
        }) as any;
        people = peopleData.people || [];
        console.log(`   Found ${people.length} decision-makers`);
      } catch (e) {
        console.warn(`   People search failed: ${(e as Error).message}`);
      }
    }

    // ✅ THE 3-WAY FALLBACK MAPPING FIX
    const personByOrgId = new Map();
    for (const p of people) {
      let mappedOrgId = p.organization_id || p.organization?.id; 
      
      // Fallback 1: Match by Website Domain
      if (!mappedOrgId && p.organization?.primary_domain) {
        const match = orgs.find(o => canonicalDomain(o.website_url) === canonicalDomain(p.organization.primary_domain));
        if (match) mappedOrgId = match.id;
      }
      
      // Fallback 2: Match by Company Name
      if (!mappedOrgId && p.organization?.name) {
        const match = orgs.find(o => o.name?.toLowerCase() === p.organization.name?.toLowerCase());
        if (match) mappedOrgId = match.id;
      }

      if (mappedOrgId && !personByOrgId.has(mappedOrgId)) {
        personByOrgId.set(mappedOrgId, p);
      }
    }
    console.log(`   Mapped ${personByOrgId.size} contacts to their companies!`);

    // ── STEP 3: Bulk-enrich emails ────────────────────────────────────────────
    console.log("   Step 3: Enriching contacts via Apollo Credits…");
    const peopleToEnrich = [...personByOrgId.values()].filter((p: any) => p.id);
    const enrichedMap = new Map<string, any>();

    for (let i = 0; i < peopleToEnrich.length; i += BATCH_SIZE) {
      const batch = peopleToEnrich.slice(i, i + BATCH_SIZE);
      try {
        const details = batch.map((p: any) => ({ id: p.id })); 
        const enrichData = await apolloPost("/people/bulk_match", { details, reveal_personal_emails: false }) as any;
        const matches = enrichData.matches || [];
        for (const m of matches) {
          if (m.id) enrichedMap.set(m.id, m);
        }
      } catch (e) {
        console.warn(`   Bulk enrich batch failed: ${(e as Error).message}`);
      }
      if (i + BATCH_SIZE < peopleToEnrich.length) {
        await new Promise((r) => setTimeout(r, RATE_LIMIT_MS));
      }
    }
    console.log(`   ✅ Enriched ${enrichedMap.size} contacts`);

    // ── STEP 4: Build rows for BOTH tables ────────────────────────────────────
    const companyRows: Record<string, unknown>[] = [];
    const leadRows: Record<string, unknown>[] = [];
    const seenDomains = new Set<string>();

    let matchedCount   = 0;
    let enrichedCount  = 0;

    for (const org of orgs) {
      const domain = canonicalDomain(org.website_url || org.primary_domain);
      if (!domain || seenDomains.has(domain)) continue;
      seenDomains.add(domain);

      const basePerson = personByOrgId.get(org.id);
      const enrichedPerson = basePerson ? enrichedMap.get(basePerson.id) : null;
      const person = enrichedPerson || basePerson;

      if (basePerson) matchedCount++;
      if (enrichedPerson) enrichedCount++;

      const email = person?.email || "";
      const { tier, tierLabel } = classifyTier(person?.email_status || (person?.has_email ? "unverified" : undefined));

      // 1. Company Table Row
      companyRows.push({
        domain,
        apollo_id:         org.id || null,
        preference_id:     pref_id,
        search_query,
        source:            "apollo",
        name:              org.name || null,
        website_url:       org.website_url || null,
        industry:          org.industry || null,
        employee_count:    org.estimated_num_employees ?? org.num_employees ?? null,
        estimated_rev:     org.annual_revenue_printed || null,
        founded_year:      org.founded_year || null,
        hq_country:        org.country || null,
        city:              org.city || null,
        state:             org.state || null,
        short_description: org.short_description || null,
        linkedin_url:      org.linkedin_url || null,
        twitter_url:       org.twitter_url || null,
        phone:             org.phone ?? org.sanitized_phone ?? null,
        contact_name:      person?.name || person?.first_name || null,
        contact_title:     person?.title || null,
        contact_email:     email || null,
        contact_email_status: person?.email_status || (person?.has_email ? "unverified" : null),
        contact_phone:     person?.phone_numbers?.[0]?.raw_number || null,
        contact_linkedin_url: person?.linkedin_url || null,
        tier,
        tier_label:       tierLabel,
        enriched:         !!enrichedPerson,
        enrichment_error: null,
        enriched_at:      enrichedPerson ? new Date().toISOString() : null,
        updated_at:       new Date().toISOString(),
      });

      // 2. Leads Table Row
      leadRows.push({
        preference_id: pref_id,
        search_query,
        domain,
        source: "apollo",
        status: "verified",
        lead_data: {
          company_name: org.name,
          website: org.website_url,
          email: email,
          phone: person?.phone_numbers?.[0]?.raw_number || org.phone || "",
          contact_name: person?.name || "",
          contact_title: person?.title || "",
          tier: email ? "tier_1_verified" : "tier_3_none",
          location: `${org.city || ''} ${org.country || ''}`.trim()
        }
      });
    }

    // ── STEP 5: Upsert into BOTH tables ───────────────────────────────────────
    if (companyRows.length > 0) {
      await supabase.from("companies").upsert(companyRows, { onConflict: "domain" });
      await supabase.from("leads").upsert(leadRows, { onConflict: "domain" });
    }

    await supabase.from("lead_preferences")
      .update({ status: "completed", completed_at: new Date().toISOString() })
      .eq("id", pref_id);

    console.log(`   🏁 Job ${pref_id} completed. Saved ${companyRows.length} total. Enriched ${enrichedCount}.`);

    return new Response(JSON.stringify({ success: true, count: companyRows.length }), { status: 200, headers: { "Content-Type": "application/json" } });

  } catch (error) {
    const msg = (error as Error).message;
    console.error("⛔ [APOLLO-PIPELINE EDGE CRASH]", msg);
    try {
      const { pref_id } = await req.clone().json();
      if (pref_id) await createClient(supabaseUrl, supabaseServiceKey).from("lead_preferences").update({ status: "failed", error_message: msg }).eq("id", pref_id);
    } catch {}
    return new Response(JSON.stringify({ error: msg }), { status: 500, headers: { "Content-Type": "application/json" } });
  }
});