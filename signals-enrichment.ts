// ═══════════════════════════════════════════════════════════════════════════════
// EDGE FUNCTION: signals-enrichment
// FILE: supabase/functions/signals-enrichment/index.ts
// ═══════════════════════════════════════════════════════════════════════════════

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const APOLLO_API_KEY     = Deno.env.get("APOLLO_API_KEY")!;

const APOLLO_BASE   = "https://api.apollo.io/api/v1";
const RATE_LIMIT_MS = 300;

const MARKETING_KEYWORDS = [
  "marketing", "growth", "brand", "demand", "seo", "sem",
  "social media", "content", "campaigns", "cmo", "acquisition",
  "performance marketing", "digital marketing", "product marketing",
];

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
    if (res.status === 404) { console.warn(`   [Apollo] 404: ${path}`); return {}; }
    if (!res.ok) { const t = await res.text(); throw new Error(`Apollo GET ${path} → ${res.status}: ${t}`); }
    return res.json();
  }
  throw new Error(`Apollo GET ${path} → max retries exceeded`);
}

function isWithinDays(dateStr: string | null | undefined, days: number): boolean {
  if (!dateStr) return false;
  try {
    const date = new Date(dateStr);
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    return date >= cutoff;
  } catch { return false; }
}

// Apollo docs: funding_events[].amount is a STRING like "5000000"
function parseFundingAmount(raw: string | number | null | undefined): number | null {
  if (raw == null) return null;
  const n = typeof raw === "number" ? raw : parseFloat(String(raw).replace(/[^0-9.]/g, ""));
  return isNaN(n) ? null : n;
}

function findMarketingRole(jobs: any[]): string | null {
  for (const job of jobs) {
    const title = (job.title || "").toLowerCase();
    if (MARKETING_KEYWORDS.some((kw) => title.includes(kw))) return job.title;
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

    const { data: companies, error: fetchErr } = await supabase
      .from("companies")
      .select("id, domain, apollo_id, name, preference_id")
      .eq("preference_id", pref_id);

    if (fetchErr) throw new Error(`Failed to fetch companies: ${fetchErr.message}`);
    if (!companies || companies.length === 0) {
      console.log("   No companies found. Nothing to enrich.");
      return new Response(JSON.stringify({ success: true, vip_count: 0 }), { status: 200, headers: { "Content-Type": "application/json" } });
    }

    console.log(`   Processing ${companies.length} companies…`);
    let vipCount = 0, fundedCount = 0, hiringCount = 0;

    for (const company of companies) {
      const { domain, apollo_id, name, id: company_id } = company;
      if (!domain) continue;

      let is_recently_funded  = false;
      let is_hiring_marketing = false;
      let funding_amount:         number | null = null;
      let funding_amount_printed: string | null = null;
      let latest_funding_date:    string | null = null;
      let role_name:              string | null = null;

      // ══════════════════════════════════════════════════════════════════════
      // STEP A: /organizations/enrich
      //
      // WHY: /mixed_companies/search does NOT return industry, annual_revenue,
      // estimated_num_employees, total_funding, short_description etc.
      // These fields are ONLY available via /organizations/enrich (per Apollo docs).
      // So we backfill ALL of them here into the companies table.
      // ══════════════════════════════════════════════════════════════════════
      try {
        const enrichData = await apolloGet(
          `/organizations/enrich?domain=${encodeURIComponent(domain)}`
        ) as any;

        const org = enrichData.organization || enrichData;

        // Log ALL funding-related fields for debugging
        console.log(`\n   💰 [FUNDING RAW] ${name} (${domain})`);
        console.log(`      latest_funding_round_date : ${org.latest_funding_round_date ?? "—"}`);
        console.log(`      latest_funding_stage      : ${org.latest_funding_stage      ?? "—"}`);
        console.log(`      total_funding             : ${org.total_funding             ?? "—"}`);
        console.log(`      total_funding_printed     : ${org.total_funding_printed     ?? "—"}`);
        console.log(`      annual_revenue            : ${org.annual_revenue            ?? "—"}`);
        console.log(`      annual_revenue_printed    : ${org.annual_revenue_printed    ?? "—"}`);
        console.log(`      industry                  : ${org.industry                  ?? "—"}`);
        console.log(`      estimated_num_employees   : ${org.estimated_num_employees   ?? "—"}`);

        // Apollo docs: funding amount lives in funding_events[0].amount (a string)
        // There is NO top-level latest_funding_amount field in Apollo's response
        const latestEvent  = (org.funding_events || [])[0] ?? null;
        const rawAmount    = latestEvent?.amount ?? null;
        const parsedAmount = parseFundingAmount(rawAmount);
        console.log(`      funding_events[0].amount  : ${rawAmount ?? "—"} → parsed: ${parsedAmount ?? "—"}`);

        // Funding signal check
        const fundingDate = org.latest_funding_round_date || null;
        if (isWithinDays(fundingDate, 30)) {
          is_recently_funded     = true;
          latest_funding_date    = fundingDate;
          funding_amount         = parsedAmount;
          funding_amount_printed = org.latest_funding_stage
            ? `${org.latest_funding_stage}${funding_amount ? ` — $${(funding_amount / 1_000_000).toFixed(1)}M` : ""}`
            : (org.total_funding_printed || null);
          fundedCount++;
          console.log(`   ✅ ${name} RECENTLY FUNDED — ${latest_funding_date} / ${funding_amount ? `$${(funding_amount/1_000_000).toFixed(1)}M` : "amount unknown"}`);
        } else {
          console.log(`   ⏭️  ${name} — not recently funded (last round: ${fundingDate ?? "no date found"})`);
        }

        // Backfill all enriched fields — these are NULL after apollo-pipeline
        // because /mixed_companies/search simply doesn't return them
        const enrichFields: Record<string, unknown> = {
          // Financials & size
          industry:              org.industry                  || null,
          employee_count:        org.estimated_num_employees   || null,
          annual_revenue:        org.annual_revenue            || null,
          estimated_rev:         org.annual_revenue_printed    || null,
          total_funding:         org.total_funding             || null,
          total_funding_printed: org.total_funding_printed     || null,
          latest_funding_stage:  org.latest_funding_stage      || null,
          latest_funding_date:   fundingDate                   || null,
          latest_funding_amount: parsedAmount                  || null,
          founded_year:          org.founded_year              || null,
          alexa_ranking:         org.alexa_ranking             || null,

          // Descriptions
          short_description: org.short_description || null,
          seo_description:   org.seo_description   || null,
          keywords:          org.keywords?.length ? org.keywords : null,

          // Location (enrich returns fuller data than search)
          city:           org.city           || null,
          state:          org.state          || null,
          hq_country:     org.country        || null,
          street_address: org.street_address || null,
          postal_code:    org.postal_code    || null,
          raw_address:    org.raw_address    || null,

          // Contact & social
          phone:         org.phone ?? org.primary_phone?.number ?? null,
          linkedin_url:  org.linkedin_url  || null,
          twitter_url:   org.twitter_url   || null,
          facebook_url:  org.facebook_url  || null,
          angellist_url: org.angellist_url  || null,
          crunchbase_url: org.crunchbase_url || null,

          // Tech stack
          tech_stack: (org.current_technologies || []).map((t: any) => ({
            name: t.name, uid: t.uid, category: t.category,
          })),

          // Signal + meta
          is_recently_funded,
          is_hiring_marketing: false, // updated below if jobs found
          enriched:    true,
          enriched_at: new Date().toISOString(),
          updated_at:  new Date().toISOString(),
        };

        // Strip nulls so we don't overwrite existing non-null data with null
        const cleanFields = Object.fromEntries(
          Object.entries(enrichFields).filter(([, v]) => v !== null && v !== undefined)
        );

        const { error: uErr } = await supabase
          .from("companies")
          .update(cleanFields)
          .eq("id", company_id);

        if (uErr) console.error(`   ❌ companies backfill failed for ${domain}: ${uErr.message}`);
        else console.log(`   ✅ ${name} — backfilled: industry=${org.industry ?? "—"}, employees=${org.estimated_num_employees ?? "—"}, revenue=${org.annual_revenue_printed ?? "—"}`);

      } catch (e) {
        console.warn(`   ⚠️  Org enrichment failed for ${domain}: ${(e as Error).message}`);
      }

      await new Promise((r) => setTimeout(r, RATE_LIMIT_MS));

      // ══════════════════════════════════════════════════════════════════════
      // STEP B: /organizations/{id}/job_postings — marketing hiring signal
      // ══════════════════════════════════════════════════════════════════════
      if (apollo_id) {
        try {
          const jobData = await apolloGet(`/organizations/${apollo_id}/job_postings`) as any;
          const jobs: any[] = jobData.job_postings || [];
          console.log(`\n   🧳 [JOBS] ${name} — ${jobs.length} postings`);
          jobs.slice(0, 5).forEach((j: any) =>
            console.log(`      • ${j.title ?? "untitled"} (${j.location ?? "?"})`));

          const marketingRole = findMarketingRole(jobs);
          if (marketingRole) {
            is_hiring_marketing = true;
            role_name           = marketingRole;
            hiringCount++;
            console.log(`   ✅ ${name} hiring: "${marketingRole}"`);
            await supabase.from("companies").update({ is_hiring_marketing: true }).eq("id", company_id);
          } else {
            console.log(`   ⏭️  ${name} — no marketing roles found`);
          }
        } catch (e) {
          console.warn(`   ⚠️  Job postings failed for ${domain}: ${(e as Error).message}`);
        }
        await new Promise((r) => setTimeout(r, RATE_LIMIT_MS));
      }

      // ══════════════════════════════════════════════════════════════════════
      // STEP C: VIP upsert if both signals true
      // ══════════════════════════════════════════════════════════════════════
      if (is_recently_funded && is_hiring_marketing) {
        vipCount++;
        console.log(`\n   🌟 VIP: ${name} — funded + hiring!`);
        const { error: vipErr } = await supabase.from("hiring_signals").upsert({
          preference_id: pref_id,
          domain,
          company_name:           name      || null,
          apollo_id:              apollo_id || null,
          role_name,
          funding_amount,
          funding_amount_printed,
          latest_funding_date,
        }, { onConflict: "domain" });
        if (vipErr) console.error(`   ❌ hiring_signals upsert failed: ${vipErr.message}`);
      }
    }

    console.log(`\n   🏁 Done. Funded: ${fundedCount}, Hiring: ${hiringCount}, VIP: ${vipCount}`);

    return new Response(
      JSON.stringify({ success: true, total: companies.length, funded_count: fundedCount, hiring_count: hiringCount, vip_count: vipCount }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );

  } catch (error) {
    const msg = (error as Error).message;
    console.error("⛔ [SIGNALS-ENRICHMENT CRASH]", msg);
    return new Response(JSON.stringify({ error: msg }), { status: 500, headers: { "Content-Type": "application/json" } });
  }
});