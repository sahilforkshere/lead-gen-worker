import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const APOLLO_API_KEY     = Deno.env.get("APOLLO_API_KEY")!;

const APOLLO_BASE    = "https://api.apollo.io/api/v1";
const MAX_COMPANIES  = 30;
const BATCH_SIZE     = 10;   // bulk_match hard limit is 10
const RATE_LIMIT_MS  = 300;

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

// ── 2. APOLLO FETCHER with detailed error logging ─────────────────────────────
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
      console.error(`❌ Apollo ${path} → ${res.status}: ${errorText}`);
      throw new Error(`Apollo ${path} → ${res.status}: ${errorText}`);
    }
    return res.json();
  }
  throw new Error(`Apollo ${path} → max retries exceeded`);
}

// ── 3. TIER CLASSIFICATION ────────────────────────────────────────────────────
function classifyTier(emailStatus: string | undefined): { tier: string; tierLabel: string } {
  if (emailStatus === "verified")         return { tier: "tier_1_verified",   tierLabel: "Verified email"   };
  if (emailStatus === "likely_to_engage") return { tier: "tier_2_email",      tierLabel: "Likely email"     };
  if (emailStatus && emailStatus !== "invalid" && emailStatus !== "do_not_email")
                                          return { tier: "tier_2_email",      tierLabel: "Unverified email" };
  return                                  { tier: "tier_3_linkedin_only", tierLabel: "LinkedIn only"    };
}

// ── 4. FILTER BUILDER ─────────────────────────────────────────────────────────
function buildFilters(search_query: string): Record<string, unknown> {
  let keywords = search_query.trim();
  const locationTags: string[] = [];

  const inMatch = search_query.match(/\bin\s+([A-Za-z\s]+)$/i);
  if (inMatch) {
    locationTags.push(inMatch[1].trim());
    keywords = search_query.replace(inMatch[0], "").trim();
  }

  const keywordTags = keywords.split(/\s+/).filter((w) => w.length > 1);
  if (keywordTags.length === 0) keywordTags.push(keywords);

  const filters: Record<string, unknown> = {
    q_organization_keyword_tags: keywordTags,
    per_page: MAX_COMPANIES,
    page: 1,
  };
  if (locationTags.length > 0) filters.organization_locations = locationTags;
  return filters;
}

// ═════════════════════════════════════════════════════════════════════════════
// MAIN EDGE FUNCTION
// ═════════════════════════════════════════════════════════════════════════════
serve(async (req) => {
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    const { pref_id, user_id, search_query } = await req.json();
    if (!pref_id || !user_id || !search_query) throw new Error("Missing required fields");

    console.log(`\n🔵 [APOLLO-PIPELINE] Job: ${pref_id} | Query: "${search_query}"`);

    // ── STEP 1: Search companies ──────────────────────────────────────────────
    console.log("   Step 1: Searching companies…");
    const orgData = await apolloPost("/mixed_companies/search", buildFilters(search_query)) as any;
    const orgs: any[] = orgData.organizations || [];
    console.log(`   Found ${orgs.length} companies`);

    if (orgs.length === 0) {
      await supabase.from("lead_preferences").update({ status: "completed", completed_at: new Date().toISOString() }).eq("id", pref_id);
      return new Response(JSON.stringify({ success: true, count: 0, message: "No companies found" }), { status: 200, headers: { "Content-Type": "application/json" } });
    }

    // ── STEP 2: Find decision-makers by org ID ────────────────────────────────
    console.log("   Step 2: Finding decision-makers…");
    const orgIds = orgs.map((o: any) => o.id).filter(Boolean);
    let people: any[] = [];

    if (orgIds.length > 0) {
      try {
        const peopleData = await apolloPost("/mixed_people/api_search", {
          organization_ids:   orgIds,
          person_titles:      ["CEO", "Founder", "Co-Founder", "Partner", "Managing Director", "President", "Owner", "Director"],
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

    // ── Map best person per org ID (3-way fallback) ───────────────────────────
    const personByOrgId = new Map<string, any>();
    for (const p of people) {
      let mappedOrgId = p.organization_id || p.organization?.id;

      if (!mappedOrgId && p.organization?.primary_domain) {
        const match = orgs.find((o) => canonicalDomain(o.website_url) === canonicalDomain(p.organization.primary_domain));
        if (match) mappedOrgId = match.id;
      }
      if (!mappedOrgId && p.organization?.name) {
        const match = orgs.find((o) => o.name?.toLowerCase() === p.organization.name?.toLowerCase());
        if (match) mappedOrgId = match.id;
      }

      if (!mappedOrgId) continue;

      // Keep the person most likely to have real data
      if (!personByOrgId.has(mappedOrgId)) {
        personByOrgId.set(mappedOrgId, p);
      } else {
        const existing  = personByOrgId.get(mappedOrgId);
        const scoreNew  = (p.has_email ? 2 : 0) + (p.has_direct_phone === "Yes" ? 1 : 0);
        const scoreOld  = (existing.has_email ? 2 : 0) + (existing.has_direct_phone === "Yes" ? 1 : 0);
        if (scoreNew > scoreOld) personByOrgId.set(mappedOrgId, p);
      }
    }
    console.log(`   Mapped ${personByOrgId.size} / ${orgs.length} contacts to companies`);

    // ── STEP 3: Bulk-enrich — send MAXIMUM context to Apollo ─────────────────
    // ✅ FIX: passing id + first_name + domain + org_name + linkedin_url
    // gives Apollo the "detective clues" it needs to unlock the full profile.
    // Sending only { id } causes many silent mismatches.
    console.log("   Step 3: Enriching contacts via bulk_match…");
    const allPeople = [...personByOrgId.values()].filter((p: any) => p.id);
    const enrichedMap = new Map<string, any>();

    for (let i = 0; i < allPeople.length; i += BATCH_SIZE) {
      const batch = allPeople.slice(i, i + BATCH_SIZE);
      try {
        const details = batch.map((p: any) => {
          const orgDomain = canonicalDomain(
            p.organization?.primary_domain || p.organization?.website_url
          );
          return {
            id:                p.id,
            first_name:        p.first_name         || undefined,
            // NOTE: last_name_obfuscated from search — don't send it; send name instead
            organization_name: p.organization?.name || undefined,
            domain:            orgDomain             || undefined,
            linkedin_url:      p.linkedin_url        || undefined,
          };
        });

        const enrichData = await apolloPost("/people/bulk_match", {
          details,
          reveal_personal_emails: false,
        }) as any;

        console.log(`   bulk_match batch ${i}: ${enrichData.unique_enriched_records ?? "?"} enriched / ${enrichData.missing_records ?? "?"} missing`);

        for (const m of (enrichData.matches || [])) {
          if (m.id) enrichedMap.set(m.id, m);
        }
      } catch (e) {
        console.warn(`   Enrich batch ${i} failed: ${(e as Error).message}`);
      }
      if (i + BATCH_SIZE < allPeople.length) await new Promise((r) => setTimeout(r, RATE_LIMIT_MS));
    }
    console.log(`   ✅ Enriched ${enrichedMap.size} contacts total`);

    // ── STEP 4: Build rows ────────────────────────────────────────────────────
    const companyRows: Record<string, unknown>[] = [];
    const leadRows:    Record<string, unknown>[] = [];
    const seenDomains = new Set<string>();
    let matchedCount = 0, enrichedCount = 0;

    for (const org of orgs) {
      const domain = canonicalDomain(org.website_url || org.primary_domain);
      if (!domain || seenDomains.has(domain)) continue;
      seenDomains.add(domain);

      const basePerson    = personByOrgId.get(org.id);
      const enrichedPerson = basePerson ? enrichedMap.get(basePerson.id) : null;

      // Use enriched data first, fall back to search-level data
      const person = enrichedPerson || basePerson;

      if (basePerson)    matchedCount++;
      if (enrichedPerson) enrichedCount++;

      const { tier, tierLabel } = classifyTier(
        enrichedPerson?.email_status || (basePerson?.has_email ? "unverified" : undefined)
      );

      // ── Extract ALL available org fields ─────────────────────────────────
      companyRows.push({
        // Keys
        domain,
        apollo_id:      org.id          || null,
        preference_id:  pref_id,
        search_query,
        source:         "apollo",

        // Company identity
        name:              org.name                    || null,
        website_url:       org.website_url             || null,
        logo_url:          org.logo_url                || null,    // ✅ Added

        // Industry & description
        industry:          org.industry                || null,
        keywords:          org.keywords                || [],      // ✅ Added — array of keyword strings
        short_description: org.short_description       || null,
        seo_description:   org.seo_description         || null,    // ✅ Added

        // Size & financials
        employee_count:    org.estimated_num_employees ?? org.num_employees ?? null,
        estimated_rev:     org.annual_revenue_printed  || null,
        annual_revenue:    org.annual_revenue          || null,    // ✅ Added — raw number
        total_funding:     org.total_funding           || null,    // ✅ Added
        total_funding_printed: org.total_funding_printed || null,  // ✅ Added
        latest_funding_stage:  org.latest_funding_stage  || null,  // ✅ Added
        latest_funding_date:   org.latest_funding_round_date || null, // ✅ Added
        founded_year:      org.founded_year            || null,
        alexa_ranking:     org.alexa_ranking           || null,    // ✅ Added

        // Location
        hq_country:        org.country                 || null,
        city:              org.city                    || null,
        state:             org.state                   || null,
        street_address:    org.street_address          || null,    // ✅ Added
        postal_code:       org.postal_code             || null,    // ✅ Added
        raw_address:       org.raw_address             || null,    // ✅ Added

        // Social / web
        linkedin_url:      org.linkedin_url            || null,
        twitter_url:       org.twitter_url             || null,
        facebook_url:      org.facebook_url            || null,    // ✅ Added
        angellist_url:     org.angellist_url            || null,   // ✅ Added
        crunchbase_url:    org.crunchbase_url           || null,   // ✅ Added
        phone:             org.phone ?? org.sanitized_phone ?? null,

        // Tech stack
        tech_stack: (org.current_technologies || org.technologies || []).map(
          (t: any) => ({ name: t.name, uid: t.uid, category: t.category })
        ),                                                          // ✅ Added category

        // ── Extract ALL available contact fields ──────────────────────────
        contact_name:         enrichedPerson?.name        || basePerson?.name        || basePerson?.first_name || null,
        contact_first_name:   enrichedPerson?.first_name  || basePerson?.first_name  || null,  // ✅ Added
        contact_last_name:    enrichedPerson?.last_name                               || null,  // ✅ Added (only available after enrich)
        contact_title:        enrichedPerson?.title        || basePerson?.title        || null,
        contact_headline:     enrichedPerson?.headline                                 || null,  // ✅ Added
        contact_photo_url:    enrichedPerson?.photo_url                                || null,  // ✅ Added
        contact_seniority:    enrichedPerson?.seniority    || basePerson?.seniority    || null,  // ✅ Added
        contact_departments:  enrichedPerson?.departments  || basePerson?.departments  || [],    // ✅ Added
        contact_city:         enrichedPerson?.city                                     || null,  // ✅ Added
        contact_state:        enrichedPerson?.state                                    || null,  // ✅ Added
        contact_country:      enrichedPerson?.country                                  || null,  // ✅ Added
        contact_email:        enrichedPerson?.email                                    || null,
        contact_email_status: enrichedPerson?.email_status
                              || (basePerson?.has_email === true ? "unverified" : null),
        // Phone: prefer direct phone, then any number from array
        contact_phone: enrichedPerson?.phone_numbers?.find((ph: any) => ph.type === "work_hq")?.raw_number
                    || enrichedPerson?.phone_numbers?.[0]?.raw_number
                    || null,                                                            // ✅ Improved
        contact_linkedin_url: enrichedPerson?.linkedin_url || basePerson?.linkedin_url || null,

        tier,
        tier_label:       tierLabel,
        enriched:         !!enrichedPerson,
        enrichment_error: null,
        enriched_at:      enrichedPerson ? new Date().toISOString() : null,
        updated_at:       new Date().toISOString(),
      });

      // ── Leads table row ───────────────────────────────────────────────────
      leadRows.push({
        preference_id: pref_id,
        search_query,
        domain,
        source: "apollo",
        status: enrichedPerson?.email_status === "verified" ? "verified" : "unverified",
        lead_data: {
          company_name:   org.name,
          website:        org.website_url,
          email:          enrichedPerson?.email || "",
          phone:          enrichedPerson?.phone_numbers?.[0]?.raw_number || org.phone || "",
          contact_name:   person?.name || "",
          contact_title:  person?.title || "",
          tier,
          location:       [org.city, org.country].filter(Boolean).join(", "),
          funding:        org.total_funding_printed || null,
          employees:      org.estimated_num_employees || null,
        },
      });
    }

    console.log(`   Built ${companyRows.length} rows | contacts: ${matchedCount} matched, ${enrichedCount} enriched`);

    // ── STEP 5: Upsert ────────────────────────────────────────────────────────
    if (companyRows.length > 0) {
      const { error: compErr } = await supabase.from("companies").upsert(companyRows, { onConflict: "domain" });
      if (compErr) throw new Error(`companies upsert: ${compErr.message}`);

      const { error: leadErr } = await supabase.from("leads").upsert(leadRows, { onConflict: "domain" });
      if (leadErr) console.warn(`leads upsert warning: ${leadErr.message}`);
    }

    // ── STEP 6: Mark completed ────────────────────────────────────────────────
    await supabase.from("lead_preferences")
      .update({ status: "completed", completed_at: new Date().toISOString() })
      .eq("id", pref_id);

    console.log(`   🏁 Job ${pref_id} done. Saved ${companyRows.length}, enriched ${enrichedCount}.`);

    return new Response(
      JSON.stringify({ success: true, companies_saved: companyRows.length, matched_contacts: matchedCount, enriched_contacts: enrichedCount }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );

  } catch (error) {
    const msg = (error as Error).message;
    console.error("⛔ [APOLLO-PIPELINE CRASH]", msg);
    try {
      const { pref_id } = await req.clone().json();
      if (pref_id) await createClient(supabaseUrl, supabaseServiceKey).from("lead_preferences").update({ status: "failed", error_message: msg }).eq("id", pref_id);
    } catch { /* ignore */ }
    return new Response(JSON.stringify({ error: msg }), { status: 500, headers: { "Content-Type": "application/json" } });
  }
});