import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const APOLLO_API_KEY     = Deno.env.get("APOLLO_API_KEY")!;

const APOLLO_BASE   = "https://api.apollo.io/api/v1";
const MAX_COMPANIES = 5;
const BATCH_SIZE    = 10;
const RATE_LIMIT_MS = 300;

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
  if (emailStatus === "verified")         return { tier: "tier_1_verified",      tierLabel: "Verified email"   };
  if (emailStatus === "likely_to_engage") return { tier: "tier_2_email",         tierLabel: "Likely email"     };
  if (emailStatus && emailStatus !== "invalid" && emailStatus !== "do_not_email")
                                          return { tier: "tier_2_email",         tierLabel: "Unverified email" };
  return                                  { tier: "tier_3_linkedin_only", tierLabel: "LinkedIn only"    };
}

// ── 4. LOCATION HELPERS ───────────────────────────────────────────────────────

const US_STATES: Record<string, string> = {
  "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
  "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
  "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
  "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
  "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
  "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
  "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
  "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
  "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
  "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
  "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
  "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
  "wisconsin": "WI", "wyoming": "WY", "washington dc": "DC", "washington d.c.": "DC",
};

const COUNTRY_CODES: Record<string, string> = {
  "united states": "US", "usa": "US", "us": "US", "america": "US",
  "united kingdom": "GB", "uk": "GB", "england": "GB",
  "canada": "CA", "australia": "AU", "germany": "DE", "france": "FR",
  "india": "IN", "singapore": "SG", "netherlands": "NL", "sweden": "SE",
  "israel": "IL", "brazil": "BR", "japan": "JP", "south korea": "KR",
  "uae": "AE", "united arab emirates": "AE", "dubai": "AE",
  "spain": "ES", "italy": "IT", "switzerland": "CH", "ireland": "IE",
  "denmark": "DK", "norway": "NO", "finland": "FI", "austria": "AT",
  "new zealand": "NZ", "mexico": "MX", "china": "CN",
};

const US_CITIES: Record<string, string> = {
  "san francisco": "CA", "new york": "NY", "new york city": "NY", "nyc": "NY",
  "los angeles": "CA", "seattle": "WA", "boston": "MA", "chicago": "IL",
  "austin": "TX", "denver": "CO", "miami": "FL", "atlanta": "GA",
  "dallas": "TX", "houston": "TX", "phoenix": "AZ", "portland": "OR",
  "san diego": "CA", "san jose": "CA", "nashville": "TN", "raleigh": "NC",
  "minneapolis": "MN", "detroit": "MI", "philadelphia": "PA", "las vegas": "NV",
  "salt lake city": "UT", "washington": "DC", "dc": "DC",
};

interface ParsedLocation {
  rawText: string;
  apolloLocation: string;
  countryCode: string | null;
}

function parseLocation(raw: string): ParsedLocation {
  const lower = raw.trim().toLowerCase();

  if (COUNTRY_CODES[lower]) {
    const code = COUNTRY_CODES[lower];
    const countryNames: Record<string, string> = {
      "US": "United States", "GB": "United Kingdom", "CA": "Canada",
      "AU": "Australia", "DE": "Germany", "FR": "France", "IN": "India",
      "SG": "Singapore", "NL": "Netherlands", "SE": "Sweden",
      "IL": "Israel", "BR": "Brazil", "JP": "Japan", "KR": "South Korea",
      "AE": "United Arab Emirates", "ES": "Spain", "IT": "Italy",
      "CH": "Switzerland", "IE": "Ireland", "DK": "Denmark",
      "NO": "Norway", "FI": "Finland", "AT": "Austria",
      "NZ": "New Zealand", "MX": "Mexico", "CN": "China",
    };
    return {
      rawText:        raw,
      apolloLocation: countryNames[code] ?? raw,
      countryCode:    code,
    };
  }

  if (US_STATES[lower]) {
    return {
      rawText:        raw,
      apolloLocation: `${raw}, United States`,
      countryCode:    "US",
    };
  }

  if (US_CITIES[lower]) {
    const stateCode = US_CITIES[lower];
    const stateName = Object.entries(US_STATES).find(([, v]) => v === stateCode)?.[0] ?? stateCode;
    const stateFormatted = stateName
      .split(" ")
      .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
      .join(" ");
    return {
      rawText:        raw,
      apolloLocation: `${raw}, ${stateFormatted}, United States`,
      countryCode:    "US",
    };
  }

  return {
    rawText:        raw,
    apolloLocation: raw,
    countryCode:    null,
  };
}

// ── 5. FILTER BUILDER ─────────────────────────────────────────────────────────
function buildFilters(search_query: string): Record<string, unknown> {
  const trimmed = search_query.trim();

  const inMatch = trimmed.match(/\bin\s+(.+)$/i);
  let keywords    = trimmed;
  let parsedLoc: ParsedLocation | null = null;

  if (inMatch) {
    const locationText = inMatch[1].trim();
    parsedLoc = parseLocation(locationText);
    keywords  = trimmed.replace(inMatch[0], "").trim();
    console.log(`   🗺️  Location parsed: "${locationText}" → "${parsedLoc.apolloLocation}" (country: ${parsedLoc.countryCode ?? "unknown"})`);
  }

  const stopWords = new Set(["in", "at", "the", "a", "an", "for", "of", "and", "or", "with"]);
  const keywordTags = keywords
    .split(/\s+/)
    .filter((w) => w.length > 1 && !stopWords.has(w.toLowerCase()));

  const allKeywords = keywordTags.length > 0 ? keywordTags : [keywords];

  const filters: Record<string, unknown> = {
    q_organization_keyword_tags: allKeywords,
    per_page: MAX_COMPANIES,
    page: 1,
  };

  if (parsedLoc) {
    filters.organization_locations = [parsedLoc.apolloLocation];
  }

  if (parsedLoc?.countryCode) {
    filters.organization_country_codes = [parsedLoc.countryCode];
  }

  console.log(`   🔍 Apollo filters:`, JSON.stringify(filters, null, 2));
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

    orgs.forEach((o: any) => {
      console.log(`   🏢 ${o.name ?? "?"} | ${o.city ?? "?"}, ${o.country ?? "?"} | ${o.website_url ?? "?"}`);
    });

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

    // ── Map best person per org ID ────────────────────────────────────────────
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

    // ── STEP 3: Bulk-enrich contacts ──────────────────────────────────────────
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
    const leadRows: Record<string, unknown>[] = [];
    const seenDomains = new Set<string>();
    let matchedCount = 0, enrichedCount = 0;

    for (const org of orgs) {
      const domain = canonicalDomain(org.website_url || org.primary_domain);
      if (!domain || seenDomains.has(domain)) continue;
      seenDomains.add(domain);

      const basePerson     = personByOrgId.get(org.id);
      const enrichedPerson = basePerson ? enrichedMap.get(basePerson.id) : null;
      const person         = enrichedPerson || basePerson;

      if (basePerson)     matchedCount++;
      if (enrichedPerson) enrichedCount++;

      const { tier, tierLabel } = classifyTier(
        enrichedPerson?.email_status || (basePerson?.has_email ? "unverified" : undefined)
      );

      const phone =
        enrichedPerson?.phone_numbers?.find((ph: any) => ph.type === "work_hq")?.raw_number ||
        enrichedPerson?.phone_numbers?.[0]?.raw_number ||
        org.phone ||
        org.sanitized_phone ||
        null;

      leadRows.push({
        preference_id: pref_id,
        search_query,
        domain,
        source:        "apollo",
        has_website:   !!(org.website_url),
        review_count:  null,
        drip_source:   null,
        company_name:  org.name || null,
        phone,
        address:       org.raw_address || [org.street_address, org.city, org.state, org.country].filter(Boolean).join(", ") || null,
        listing_url:   org.linkedin_url || null,
        status:        enrichedPerson?.email_status === "verified" ? "verified" : "unverified",
        lead_data: {
          company_name:         org.name                                    || null,
          website:              org.website_url                             || null,
          email:                enrichedPerson?.email                       || null,
          phone,
          contact_name:         person?.name                                || null,
          contact_first_name:   enrichedPerson?.first_name || basePerson?.first_name || null,
          contact_last_name:    enrichedPerson?.last_name                   || null,
          contact_title:        person?.title                               || null,
          contact_headline:     enrichedPerson?.headline                    || null,
          contact_photo_url:    enrichedPerson?.photo_url                   || null,
          contact_seniority:    enrichedPerson?.seniority || basePerson?.seniority || null,
          contact_departments:  enrichedPerson?.departments || basePerson?.departments || [],
          contact_city:         enrichedPerson?.city                        || null,
          contact_state:        enrichedPerson?.state                       || null,
          contact_country:      enrichedPerson?.country                     || null,
          contact_email:        enrichedPerson?.email                       || null,
          contact_email_status: enrichedPerson?.email_status || (basePerson?.has_email === true ? "unverified" : null),
          contact_phone:        phone,
          contact_linkedin_url: enrichedPerson?.linkedin_url || basePerson?.linkedin_url || null,
          tier,
          tier_label:           tierLabel,
          location:             [org.city, org.country].filter(Boolean).join(", ") || null,
          address:              org.raw_address || null,
          industry:             org.industry                                || null,
          keywords:             org.keywords                                || [],
          short_description:    org.short_description                       || null,
          seo_description:      org.seo_description                        || null,
          employee_count:       org.estimated_num_employees ?? org.num_employees ?? null,
          estimated_rev:        org.annual_revenue_printed                  || null,
          annual_revenue:       org.annual_revenue                          || null,
          total_funding:        org.total_funding                           || null,
          total_funding_printed: org.total_funding_printed                  || null,
          latest_funding_stage: org.latest_funding_stage                   || null,
          latest_funding_date:  org.latest_funding_round_date              || null,
          founded_year:         org.founded_year                            || null,
          alexa_ranking:        org.alexa_ranking                          || null,
          hq_country:           org.country || person?.organization?.country || null,
          city:                 org.city || person?.organization?.city      || null,
          state:                org.state                                   || null,
          street_address:       org.street_address                         || null,
          postal_code:          org.postal_code                             || null,
          logo_url:             org.logo_url                                || null,
          linkedin_url:         org.linkedin_url                            || null,
          twitter_url:          org.twitter_url                             || null,
          facebook_url:         org.facebook_url                            || null,
          angellist_url:        org.angellist_url                           || null,
          crunchbase_url:       org.crunchbase_url                          || null,
          tech_stack: (org.current_technologies || org.technologies || []).map(
            (t: any) => ({ name: t.name, uid: t.uid, category: t.category })
          ),
          enriched:             !!enrichedPerson,
          enriched_at:          enrichedPerson ? new Date().toISOString() : null,
          apollo_id:            org.id || null,
          source:               "apollo",
        },
        updated_at: new Date().toISOString(),
      });
    }

    console.log(`   Built ${leadRows.length} rows | contacts: ${matchedCount} matched, ${enrichedCount} enriched`);

    // ── STEP 5: Upsert into leads only ───────────────────────────────────────
    if (leadRows.length > 0) {
      const { error: leadErr } = await supabase
        .from("leads")
        .upsert(leadRows, { onConflict: "domain" });
      if (leadErr) throw new Error(`leads upsert: ${leadErr.message}`);
    }

    // ── STEP 6: Mark completed ────────────────────────────────────────────────
    await supabase.from("lead_preferences")
      .update({ status: "completed", completed_at: new Date().toISOString() })
      .eq("id", pref_id);

    console.log(`   🏁 Job ${pref_id} done. Saved ${leadRows.length}, enriched ${enrichedCount}.`);

    // ── STEP 7: Fire signals-enrichment (fire-and-forget) ─────────────────────
    fetch(`${supabaseUrl}/functions/v1/signals-enrichment`, {
      method:  "POST",
      headers: {
        "Content-Type":  "application/json",
        "Authorization": `Bearer ${supabaseServiceKey}`,
      },
      body: JSON.stringify({ pref_id }),
    }).catch((e) =>
      console.error("   signals-enrichment fire error:", e.message)
    );

    return new Response(
      JSON.stringify({
        success:           true,
        leads_saved:       leadRows.length,
        matched_contacts:  matchedCount,
        enriched_contacts: enrichedCount,
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );

  } catch (error) {
    const msg = (error as Error).message;
    console.error("⛔ [APOLLO-PIPELINE CRASH]", msg);
    try {
      const { pref_id } = await req.clone().json();
      if (pref_id) await createClient(supabaseUrl, supabaseServiceKey)
        .from("lead_preferences")
        .update({ status: "failed", error_message: msg })
        .eq("id", pref_id);
    } catch { /* ignore */ }
    return new Response(JSON.stringify({ error: msg }), { status: 500, headers: { "Content-Type": "application/json" } });
  }
});