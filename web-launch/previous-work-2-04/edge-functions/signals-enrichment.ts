// this code is already written in sigbnakl senrichment 
// ═══════════════════════════════════════════════════════════════════════════════
// EDGE FUNCTION: signals-enrichment
//
// FILE: supabase/functions/signals-enrichment/index.ts
// DEPLOY: supabase functions deploy signals-enrichment
// SECRET: supabase secrets set SERPAPI_KEY=your_key_here
//
// HOW SerpApi Google Jobs API works:
//   GET https://serpapi.com/search
//   Params: engine=google_jobs, q="marketing jobs at {company}", api_key=...
//   Returns: { jobs_results: [{ title, company_name, location, via, description,
//               detected_extensions: { posted_at, schedule_type } }] }
//
// THIS IS THE CORRECT endpoint — NOT /search with organic results.
// engine=google_jobs hits Google's dedicated jobs card, same as what you see
// when you Google "software engineer jobs at Google".
// ═══════════════════════════════════════════════════════════════════════════════

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const SERPAPI_KEY        = Deno.env.get("SERPER_API_KEY")!;

const SERPAPI_BASE  = "https://serpapi.com/search";
const RATE_LIMIT_MS = 500; // SerpApi free: 100 searches/mo. Paid plans handle more.

// Marketing keywords — matched against job title (case-insensitive)
const MARKETING_KEYWORDS = [
  "marketing",
  "growth",
  "brand",
  "demand generation",
  "seo", "sem",
  "social media",
  "content",
  "cmo",
  "acquisition",
  "performance marketing",
  "digital marketing",
  "product marketing",
  "email marketing",
];

// ── SerpApi Google Jobs fetcher ───────────────────────────────────────────────
async function fetchJobsFromSerpApi(companyName: string): Promise<any[]> {
  // Query: "marketing jobs at {company name}"
  // engine=google_jobs returns Google's structured jobs card results
  const query = `marketing jobs at ${companyName}`;

  const params = new URLSearchParams({
    engine:  "google_jobs",
    q:       query,
    api_key: SERPAPI_KEY,
    hl:      "en",
    gl:      "us",  // change to "gb" for UK-focused, "in" for India
  });

  const res = await fetch(`${SERPAPI_BASE}?${params.toString()}`, {
    method:  "GET",
    headers: { "Content-Type": "application/json" },
  });

  if (res.status === 429) throw new Error("SerpApi rate limit hit");
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`SerpApi error ${res.status}: ${text}`);
  }

  const data = await res.json() as any;

  // ✅ jobs_results is the correct field — NOT organic, NOT jobs
  return data.jobs_results || [];
}

// ── Find marketing role from jobs_results ─────────────────────────────────────
// Returns { roleName, allMarketingRoles } so we can log all found roles
function findMarketingRoles(jobs: any[], companyName: string): {
  roleName: string | null;
  allMarketingRoles: string[];
} {
  const allMarketingRoles: string[] = [];

  for (const job of jobs) {
    const title = (job.title || "").toLowerCase();
    const isMarketing = MARKETING_KEYWORDS.some((kw) => title.includes(kw));

    if (isMarketing) {
      // Only count jobs actually at this company (SerpApi may return similar roles elsewhere)
      const jobCompany = (job.company_name || "").toLowerCase();
      const targetName = companyName.toLowerCase();

      // Fuzzy match: company name contains search name or vice versa
      if (
        jobCompany.includes(targetName.split(" ")[0]) ||
        targetName.includes(jobCompany.split(" ")[0]) ||
        jobCompany === targetName
      ) {
        allMarketingRoles.push(job.title);
      }
    }
  }

  return {
    roleName:         allMarketingRoles[0] || null,
    allMarketingRoles,
  };
}

// ═════════════════════════════════════════════════════════════════════════════
serve(async (req) => {
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    const { pref_id } = await req.json();
    if (!pref_id) throw new Error("Missing pref_id");

    console.log(`\n${"═".repeat(60)}`);
    console.log(`📡 [SIGNALS-ENRICHMENT] pref_id: ${pref_id}`);
    console.log(`${"═".repeat(60)}`);

    // ── Load companies for this pref_id ──────────────────────────────────────
    const { data: companies, error: fetchErr } = await supabase
      .from("companies")
      .select("id, domain, apollo_id, name, preference_id, industry, hq_country")
      .eq("preference_id", pref_id);

    if (fetchErr) throw new Error(`Failed to fetch companies: ${fetchErr.message}`);

    if (!companies || companies.length === 0) {
      console.log("   ⚠️  No companies found for this pref_id.");
      return new Response(
        JSON.stringify({ success: true, vip_count: 0, message: "No companies to process" }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    console.log(`\n   📋 Companies to process: ${companies.length}`);
    companies.forEach((c, i) =>
      console.log(`   ${i + 1}. ${c.name} (${c.domain}) — ${c.hq_country || "location unknown"}`)
    );
    console.log("");

    // ── Counters ──────────────────────────────────────────────────────────────
    let hiringCount = 0;
    let vipCount    = 0;
    let errorCount  = 0;

    // ── Process each company ──────────────────────────────────────────────────
    for (const company of companies) {
      const { id: company_db_id, domain, apollo_id, name } = company;
      if (!name) {
        console.log(`   ⚠️  Skipping row ${company_db_id} — no company name`);
        continue;
      }

      console.log(`\n   ${"─".repeat(50)}`);
      console.log(`   🔍 Checking: ${name}`);
      console.log(`      domain:    ${domain || "—"}`);
      console.log(`      apollo_id: ${apollo_id || "—"}`);

      let is_hiring_marketing = false;
      let role_name:           string | null = null;
      let all_roles:           string[] = [];

      // ── SIGNAL 1: HIRING via SerpApi Google Jobs ────────────────────────────
      try {
        const jobs = await fetchJobsFromSerpApi(name);

        console.log(`\n      [SerpApi] Raw jobs returned: ${jobs.length}`);

        if (jobs.length === 0) {
          console.log(`      [SerpApi] No jobs found at all for "${name}"`);
        } else {
          // Log ALL jobs found (not just marketing) so you can see what Google returns
          console.log(`      [SerpApi] All open roles found:`);
          jobs.forEach((job, i) => {
            const postedAt = job.detected_extensions?.posted_at || "date unknown";
            const via      = job.via || "unknown source";
            console.log(`         ${i + 1}. "${job.title}" at ${job.company_name || "?"} — ${job.location || "?"} [via ${via}] [${postedAt}]`);
          });
        }

        const { roleName, allMarketingRoles } = findMarketingRoles(jobs, name);
        all_roles = allMarketingRoles;

        if (allMarketingRoles.length > 0) {
          is_hiring_marketing = true;
          role_name           = roleName;
          hiringCount++;

          console.log(`\n      ✅ MARKETING HIRING DETECTED:`);
          allMarketingRoles.forEach((r, i) =>
            console.log(`         ${i + 1}. "${r}"`)
          );
          console.log(`         → role_name saved: "${role_name}"`);
        } else {
          console.log(`\n      — No marketing roles found for "${name}"`);
        }

      } catch (e) {
        errorCount++;
        console.error(`\n      ❌ SerpApi failed for "${name}": ${(e as Error).message}`);
      }

      // ── SIGNAL 2: FUNDING — PLACEHOLDER ─────────────────────────────────────
      // Wire your custom funding signal here when ready.
      const is_recently_funded     = false;
      const funding_amount:         number | null = null;
      const funding_amount_printed: string | null = null;
      const latest_funding_date:    string | null = null;

      console.log(`\n      [Funding] PLACEHOLDER — is_recently_funded = false (not wired yet)`);

      // ── Update companies table ───────────────────────────────────────────────
      const { error: updateErr } = await supabase
        .from("companies")
        .update({ is_hiring_marketing, is_recently_funded })
        .eq("id", company_db_id);

      if (updateErr) {
        console.error(`      ❌ companies update failed: ${updateErr.message}`);
      } else {
        console.log(`\n      💾 companies table updated:`);
        console.log(`         is_hiring_marketing = ${is_hiring_marketing}`);
        console.log(`         is_recently_funded  = ${is_recently_funded}`);
      }

      // ── Push to VIP (hiring_signals) ─────────────────────────────────────────
      // Currently: push if is_hiring_marketing = true
      // When funding is wired, change to: if (is_hiring_marketing && is_recently_funded)
      if (is_hiring_marketing) {
        vipCount++;

        console.log(`\n      🌟 VIP PUSH → hiring_signals`);
        console.log(`         company:  ${name}`);
        console.log(`         domain:   ${domain}`);
        console.log(`         role:     ${role_name}`);
        console.log(`         funding:  PLACEHOLDER (null)`);

        const { error: vipErr } = await supabase
          .from("hiring_signals")
          .upsert({
            preference_id:          pref_id,
            domain,
            company_name:           name      || null,
            apollo_id:              apollo_id || null,
            role_name,
            // Funding fields — null until custom signal is wired
            funding_amount,
            funding_amount_printed,
            latest_funding_date,
          }, { onConflict: "domain" });

        if (vipErr) {
          console.error(`      ❌ hiring_signals upsert failed: ${vipErr.message}`);
        } else {
          console.log(`      ✅ hiring_signals row saved`);
        }
      }

      // Rate limit
      await new Promise((r) => setTimeout(r, RATE_LIMIT_MS));
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    console.log(`\n${"═".repeat(60)}`);
    console.log(`🏁 [SIGNALS-ENRICHMENT] Complete`);
    console.log(`   Total companies:    ${companies.length}`);
    console.log(`   Hiring marketing:   ${hiringCount}`);
    console.log(`   VIP pushed:         ${vipCount}`);
    console.log(`   Errors:             ${errorCount}`);
    console.log(`${"═".repeat(60)}\n`);

    return new Response(
      JSON.stringify({
        success:      true,
        total:        companies.length,
        hiring_count: hiringCount,
        vip_count:    vipCount,
        error_count:  errorCount,
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );

  } catch (error) {
    const msg = (error as Error).message;
    console.error(`\n⛔ [SIGNALS-ENRICHMENT CRASH] ${msg}`);
    return new Response(
      JSON.stringify({ error: msg }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
});