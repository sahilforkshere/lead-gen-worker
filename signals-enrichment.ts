// ═══════════════════════════════════════════════════════════════════════════════
// EDGE FUNCTION: signals-enrichment
//
// FILE: supabase/functions/signals-enrichment/index.ts
//
// CALLED BY: apollo-pipeline at the end (fire-and-forget)
// WHAT IT DOES:
//   1. Loads all companies saved for this pref_id
//   2. For each company:
//      a. GET /organizations/enrich?domain=  → check is_recently_funded (30 days)
//      b. GET /organizations/{apollo_id}/job_postings → check is_hiring_marketing
//   3. Updates companies table with both boolean flags
//   4. If BOTH true → upserts into hiring_signals table (VIP leads)
//
// DEPLOY: supabase functions deploy signals-enrichment
// ═══════════════════════════════════════════════════════════════════════════════

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const APOLLO_API_KEY     = Deno.env.get("APOLLO_API_KEY")!;

const APOLLO_BASE   = "https://api.apollo.io/api/v1";
const RATE_LIMIT_MS = 300; // ms between requests — GET endpoints are more lenient

// Marketing job keywords — case-insensitive match against job title
const MARKETING_KEYWORDS = [
  "marketing", "growth", "brand", "demand", "seo", "sem",
  "social media", "content", "campaigns", "cmo", "acquisition",
  "performance marketing", "digital marketing", "product marketing",
];

// ── Apollo GET fetcher ────────────────────────────────────────────────────────
async function apolloGet(path: string, retries = 3): Promise<Record<string, unknown>> {
  for (let attempt = 0; attempt < retries; attempt++) {
    const res = await fetch(`${APOLLO_BASE}${path}`, {
      method:  "GET",
      headers: { "Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY },
    });
    if (res.status === 429) {
      const wait = Math.pow(2, attempt) * 1500;
      console.warn(`   [Apollo] Rate limited on ${path}. Waiting ${wait}ms…`);
      await new Promise((r) => setTimeout(r, wait));
      continue;
    }
    if (res.status === 404) {
      // Company not found in Apollo — not an error, just skip
      return {};
    }
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Apollo GET ${path} → ${res.status}: ${text}`);
    }
    return res.json();
  }
  throw new Error(`Apollo GET ${path} → max retries exceeded`);
}

// ── Check if a date string is within the last N days ─────────────────────────
function isWithinDays(dateStr: string | null | undefined, days: number): boolean {
  if (!dateStr) return false;
  try {
    const date     = new Date(dateStr);
    const cutoff   = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    return date >= cutoff;
  } catch {
    return false;
  }
}

// ── Check job titles for marketing keywords ───────────────────────────────────
function findMarketingRole(jobs: any[]): string | null {
  for (const job of jobs) {
    const title = (job.title || "").toLowerCase();
    if (MARKETING_KEYWORDS.some((kw) => title.includes(kw))) {
      return job.title; // return the original (non-lowercased) title
    }
  }
  return null;
}

// ═════════════════════════════════════════════════════════════════════════════
serve(async (req) => {
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    const { pref_id } = await req.json();
    if (!pref_id) throw new Error("Missing pref_id");

    console.log(`\n📡 [SIGNALS-ENRICHMENT] Starting for pref_id: ${pref_id}`);

    // ── Load all companies saved for this pref_id ────────────────────────────
    const { data: companies, error: fetchErr } = await supabase
      .from("companies")
      .select("id, domain, apollo_id, name, preference_id")
      .eq("preference_id", pref_id);

    if (fetchErr) throw new Error(`Failed to fetch companies: ${fetchErr.message}`);
    if (!companies || companies.length === 0) {
      console.log("   No companies found for this pref_id. Nothing to enrich.");
      return new Response(
        JSON.stringify({ success: true, vip_count: 0, message: "No companies to process" }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    console.log(`   Processing ${companies.length} companies…`);

    let vipCount       = 0;
    let fundedCount    = 0;
    let hiringCount    = 0;

    for (const company of companies) {
      const { domain, apollo_id, name, id: company_id } = company;
      if (!domain) continue;

      let is_recently_funded   = false;
      let is_hiring_marketing  = false;
      let funding_amount:         number | null = null;
      let funding_amount_printed: string | null = null;
      let latest_funding_date:    string | null = null;
      let role_name:              string | null = null;

      // ── A: Funding check via /organizations/enrich ────────────────────────
      try {
        const enrichData = await apolloGet(
          `/organizations/enrich?domain=${encodeURIComponent(domain)}`
        ) as any;

        const org = enrichData.organization || enrichData;

        // Check latest_funding_date (most reliable field)
        const fundingDate =
          org.latest_funding_round_date ||
          org.latest_funding_date       ||
          org.funding_events?.[0]?.date || null;

        if (isWithinDays(fundingDate, 30)) {
          is_recently_funded    = true;
          latest_funding_date   = fundingDate;
          funding_amount        = org.latest_funding_amount
                               ?? org.funding_events?.[0]?.amount
                               ?? null;
          funding_amount_printed = org.latest_funding_stage
            ? `${org.latest_funding_stage}${funding_amount ? ` — $${(funding_amount / 1_000_000).toFixed(1)}M` : ""}`
            : (org.total_funding_printed || null);

          fundedCount++;
          console.log(`   ✅ ${name} funded on ${fundingDate} ($${funding_amount ?? "?"}) `);
        }
      } catch (e) {
        console.warn(`   Funding check failed for ${domain}: ${(e as Error).message}`);
      }

      await new Promise((r) => setTimeout(r, RATE_LIMIT_MS));

      // ── B: Marketing hiring check via /organizations/{id}/job_postings ────
      if (apollo_id) {
        try {
          const jobData = await apolloGet(
            `/organizations/${apollo_id}/job_postings`
          ) as any;

          const jobs: any[] = jobData.job_postings || jobData.jobs || [];
          const marketingRole = findMarketingRole(jobs);

          if (marketingRole) {
            is_hiring_marketing = true;
            role_name           = marketingRole;
            hiringCount++;
            console.log(`   ✅ ${name} hiring: "${marketingRole}"`);
          }
        } catch (e) {
          console.warn(`   Job postings check failed for ${domain} (${apollo_id}): ${(e as Error).message}`);
        }

        await new Promise((r) => setTimeout(r, RATE_LIMIT_MS));
      }

      // ── C: Update companies table with signal booleans ────────────────────
      await supabase
        .from("companies")
        .update({ is_recently_funded, is_hiring_marketing })
        .eq("id", company_id);

      // ── D: If BOTH true → upsert into hiring_signals (VIP table) ─────────
      if (is_recently_funded && is_hiring_marketing) {
        vipCount++;
        console.log(`   🌟 VIP: ${name} — funded + hiring marketing!`);

        const { error: vipErr } = await supabase
          .from("hiring_signals")
          .upsert({
            preference_id:          pref_id,
            domain,
            company_name:           name      || null,
            apollo_id:              apollo_id || null,
            role_name,
            funding_amount,
            funding_amount_printed,
            latest_funding_date,
          }, { onConflict: "domain" });

        if (vipErr) {
          console.error(`   ❌ hiring_signals upsert failed for ${domain}: ${vipErr.message}`);
        }
      }
    }

    console.log(`\n   🏁 [SIGNALS-ENRICHMENT] Done.`);
    console.log(`   Recently funded:    ${fundedCount} / ${companies.length}`);
    console.log(`   Hiring marketing:   ${hiringCount} / ${companies.length}`);
    console.log(`   VIP (both signals): ${vipCount}`);

    return new Response(
      JSON.stringify({
        success:       true,
        total:         companies.length,
        funded_count:  fundedCount,
        hiring_count:  hiringCount,
        vip_count:     vipCount,
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );

  } catch (error) {
    const msg = (error as Error).message;
    console.error("⛔ [SIGNALS-ENRICHMENT CRASH]", msg);
    return new Response(
      JSON.stringify({ error: msg }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
});