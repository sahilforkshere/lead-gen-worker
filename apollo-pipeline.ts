import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const APOLLO_API_KEY     = Deno.env.get("APOLLO_API_KEY")!;

const APOLLO_BASE    = "https://api.apollo.io/api/v1";
const MAX_COMPANIES  = 100;
const BATCH_SIZE     = 10;   // Apollo bulk_match limit
const RATE_LIMIT_MS  = 120;  // ms between batches to avoid 429s

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
  } catch {}
  const bareMatch = trimmed.toLowerCase().replace(/^www\d*\./, "").match(/^([a-z0-9-]+\.)+[a-z]{2,}$/);
  if (bareMatch) return bareMatch[0];
  return null;
}

// ── 2. APOLLO FETCHER (With Retry Logic) ──────────────────────────────────────
async function apolloPost(
  path: string,
  body: Record<string, unknown>,
  retries = 3
): Promise<Record<string, unknown>> {
  for (let attempt = 0; attempt < retries; attempt++) {
    const res = await fetch(`${APOLLO_BASE}${path}`, {
      method:  "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Api-Key":    APOLLO_API_KEY,
      },
      body: JSON.stringify(body),
    });

    if (res.status === 429) {
      const wait = Math.pow(2, attempt) * 1000;
      console.warn(`   [Apollo] Rate limited on ${path}. Waiting ${wait}ms…`);
      await new Promise((r) => setTimeout(r, wait));
      continue;
    }

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Apollo ${path} → ${res.status}: ${text}`);
    }

    return res.json();
  }
  throw new Error(`Apollo ${path} → max retries exceeded`);
}

// ── 3. TIER CLASSIFICATION ────────────────────────────────────────────────────
function classifyTier(emailStatus: string | undefined): { tier: string; tierLabel: string } {
  if (emailStatus === "verified")            return { tier: "tier_1_verified",     tierLabel: "Verified email"   };
  if (emailStatus === "likely_to_engage")    return { tier: "tier_2_email",        tierLabel: "Likely email"     };
  if (emailStatus && emailStatus !== "invalid" && emailStatus !== "do_not_email")
                                             return { tier: "tier_2_email",        tierLabel: "Unverified email" };
  return                                     { tier: "tier_3_linkedin_only", tierLabel: "LinkedIn only"    };
}

// ── 4. BUILD APOLLO FILTERS (The Array Fix) ───────────────────────────────────
function buildFilters(search_query: string): Record<string, unknown> {
  // Break "SaaS Startups in London" into an array: ["SaaS", "Startups", "London"]
  // Removes tiny filler words like "in", "of", "the" so Apollo searches better.
  const searchTags = search_query
    .split(/\s+/)
    .map(word => word.trim())
    .filter(word => word.length > 2); 

  // Fallback just in case the user typed a very short word like "AI"
  if (searchTags.length === 0) {
    searchTags.push(search_query.trim());
  }

  return {
    q_organization_keyword_tags: searchTags, // Now safely passing an Array!
    per_page: MAX_COMPANIES,
    page: 1,
  };
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

    console.log(`\n🔵 [APOLLO-PIPELINE] Job: ${pref_id}`);
    console.log(`   Query: "${search_query}"`);

    // ──────────────────────────────────────────────────────────────────────────
    // STEP 1: Search companies
    // ──────────────────────────────────────────────────────────────────────────
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

    // ──────────────────────────────────────────────────────────────────────────
    // STEP 2: Find decision-makers
    // ──────────────────────────────────────────────────────────────────────────
    console.log("   Step 2: Finding decision-makers…");

    const orgDomains = orgs
      .map((o) => canonicalDomain(o.website_url || o.primary_domain))
      .filter(Boolean) as string[];

    let people: any[] = [];
    if (orgDomains.length > 0) {
      try {
        const peopleData = await apolloPost("/mixed_people/search", {
          organization_domains: orgDomains,
          person_titles: ["CEO", "Founder", "Co-Founder", "Managing Director", "President", "Owner"],
          person_seniorities: ["owner", "founder", "c_suite"],
          contact_email_status: ["verified", "likely_to_engage"],
          per_page: MAX_COMPANIES,
          page: 1,
        }) as any;
        people = peopleData.people || [];
        console.log(`   Found ${people.length} decision-makers`);
      } catch (e) {
        console.warn(`   People search failed: ${(e as Error).message}. Continuing without contacts.`);
      }
    }

    const personByDomain = new Map<string, any>();
    for (const p of people) {
      const domain = canonicalDomain(p.organization?.primary_domain || p.organization?.website_url);
      if (domain && !personByDomain.has(domain)) {
        personByDomain.set(domain, p);
      }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // STEP 3: Bulk-enrich emails
    // ──────────────────────────────────────────────────────────────────────────
    console.log("   Step 3: Enriching emails…");

    const peopleToEnrich = [...personByDomain.values()].filter((p) => p.linkedin_url);
    const enrichedMap = new Map<string, any>(); 

    for (let i = 0; i < peopleToEnrich.length; i += BATCH_SIZE) {
      const batch = peopleToEnrich.slice(i, i + BATCH_SIZE);
      const details = batch.map((p) => ({ linkedin_url: p.linkedin_url }));

      try {
        const enrichData = await apolloPost("/people/bulk_match", { details }) as any;
        const matches: any[] = enrichData.matches || [];
        for (const m of matches) {
          if (m.linkedin_url) enrichedMap.set(m.linkedin_url, m);
        }
      } catch (e) {
        console.warn(`   Bulk enrich batch ${i}–${i + BATCH_SIZE} failed: ${(e as Error).message}`);
      }

      if (i + BATCH_SIZE < peopleToEnrich.length) {
        await new Promise((r) => setTimeout(r, RATE_LIMIT_MS));
      }
    }

    console.log(`   Enriched ${enrichedMap.size} contacts`);

    // ──────────────────────────────────────────────────────────────────────────
    // STEP 4: Build rows for companies table
    // ──────────────────────────────────────────────────────────────────────────
    const rows: Record<string, unknown>[] = [];
    const seenDomains = new Set<string>();

    for (const org of orgs) {
      const domain = canonicalDomain(org.website_url || org.primary_domain);
      if (!domain || seenDomains.has(domain)) continue;
      seenDomains.add(domain);

      const person   = personByDomain.get(domain);
      const enriched = person?.linkedin_url ? enrichedMap.get(person.linkedin_url) : undefined;
      const contact  = enriched || person;
      const { tier, tierLabel } = classifyTier(contact?.email_status);

      rows.push({
        domain,
        apollo_id:    org.id || null,
        preference_id: pref_id,
        search_query,
        source:       "apollo",
        name:          org.name || null,
        website_url:   org.website_url || null,
        industry:      org.industry || null,
        employee_count: org.num_employees || null,
        estimated_rev: org.annual_revenue_printed || null,
        founded_year:  org.founded_year || null,
        hq_country:    org.country || null,
        city:          org.city || null,
        state:         org.state || null,
        short_description: org.short_description || null,
        linkedin_url: org.linkedin_url || null,
        twitter_url:  org.twitter_url  || null,
        phone:        org.phone        || null,
        tech_stack: (org.technologies || []).map((t: any) => ({ name: t.name, uid: t.uid })),
        contact_name:          contact?.name            || null,
        contact_title:         contact?.title           || null,
        contact_email:         contact?.email           || null,
        contact_email_status:  contact?.email_status    || null,
        contact_phone:         contact?.phone_numbers?.[0]?.raw_number || null,
        contact_linkedin_url:  contact?.linkedin_url    || null,
        tier,
        tier_label: tierLabel,
        enriched:         !!enriched,
        enrichment_error: null,
        enriched_at:      enriched ? new Date().toISOString() : null,
      });
    }

    console.log(`   Built ${rows.length} company rows`);

    // ──────────────────────────────────────────────────────────────────────────
    // STEP 5: Upsert into companies table
    // ──────────────────────────────────────────────────────────────────────────
    if (rows.length > 0) {
      const { error: upsertErr } = await supabase
        .from("companies")
        .upsert(rows, { onConflict: "domain" });

      if (upsertErr) {
        console.error("   ❌ companies upsert error:", upsertErr.message);
        throw new Error(`companies upsert failed: ${upsertErr.message}`);
      }

      console.log(`   ✅ ${rows.length} companies saved to DB`);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // STEP 6: Mark completed
    // ──────────────────────────────────────────────────────────────────────────
    await supabase.from("lead_preferences")
      .update({
        status:       "completed",
        completed_at: new Date().toISOString(),
      })
      .eq("id", pref_id);

    console.log(`   ✅ Job ${pref_id} completed`);

    return new Response(
      JSON.stringify({ success: true, companies_saved: rows.length }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );

  } catch (error) {
    const msg = (error as Error).message;
    console.error("⛔ [APOLLO-PIPELINE]", msg);

    if (req.body) {
      try {
        const { pref_id } = await req.clone().json();
        if (pref_id) {
          await createClient(supabaseUrl, supabaseServiceKey)
            .from("lead_preferences")
            .update({ status: "failed", error_message: msg })
            .eq("id", pref_id);
        }
      } catch { /* ignore */ }
    }

    return new Response(
      JSON.stringify({ error: msg }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
});