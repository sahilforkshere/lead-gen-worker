// ═══════════════════════════════════════════════════════════════════════════════
// EDGE FUNCTION: apollo-tech-scanner
// FILE: supabase/functions/apollo-tech-scanner/index.ts
// ═══════════════════════════════════════════════════════════════════════════════

import { serve }        from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const APOLLO_API_KEY     = Deno.env.get("APOLLO_API_KEY")!; // ⚠️ MUST BE A MASTER API KEY

const APOLLO_BASE   = "https://api.apollo.io/api/v1";

// ── 1. APOLLO FETCHER (Handles 429s and errors safely) ────────────────────────
async function apolloPost(path: string, body: Record<string, unknown>, retries = 3): Promise<Record<string, unknown>> {
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
    
    if (!res.ok) {
      const errorText = await res.text();
      throw new Error(`Apollo ${path} → ${res.status}: ${errorText}`);
    }
    return res.json();
  }
  throw new Error(`Apollo ${path} → max retries exceeded`);
}

// ── 2. DOMAIN & LOCATION PARSERS ──────────────────────────────────────────────
function canonicalDomain(raw: string | null | undefined): string | null {
  if (!raw || typeof raw !== "string") return null;
  const trimmed = raw.trim();
  if (!trimmed) return null;
  const withProtocol = trimmed.startsWith("http") ? trimmed : `https://${trimmed}`;
  try {
    let hostname = new URL(withProtocol).hostname.toLowerCase().replace(/^www\d*\./, "");
    if (hostname && hostname.includes(".")) return hostname;
  } catch { /* fall through */ }
  return null;
}

const US_STATES: Record<string, string> = { "florida": "FL", "california": "CA", "texas": "TX", "new york": "NY" };
const COUNTRY_CODES: Record<string, string> = { "united kingdom": "GB", "uk": "GB", "united states": "US", "usa": "US" };

function parseLocation(raw: string): string {
  const lower = raw.trim().toLowerCase();
  if (COUNTRY_CODES[lower]) {
    const names: Record<string, string> = { "US": "United States", "GB": "United Kingdom" };
    return names[COUNTRY_CODES[lower]] ?? raw;
  }
  if (US_STATES[lower]) return `${raw}, United States`;
  return raw; // Fallback
}

// ── 3. MAIN PIPELINE ─────────────────────────────────────────────────────────
serve(async (req) => {
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    const {
      user_id,
      search_query,      // e.g., "Software"
      location,          // e.g., "San Francisco"
      filters = {},      // e.g., { employee_range: ["11,50"] }
      preference_id,
      mechanism
    } = await req.json();

    console.log(`\n🚀 [APOLLO-TECH-SCANNER] Executing...`);

    // ── Phase 1: Build Safe Apollo Payload ───────────────────────────────────
    const apolloPayload: Record<string, any> = {
      page: 1,
      per_page: 15
    };

    // 🎯 Master API Key allows advanced tagging
    if (search_query) {
      apolloPayload.q_organization_keyword_tags = [search_query.trim()];
    }
    
    if (location) {
      apolloPayload.organization_locations = [parseLocation(location)];
    }

    if (filters.employee_range && Array.isArray(filters.employee_range) && filters.employee_range.length > 0) {
      apolloPayload.organization_num_employees_ranges = filters.employee_range;
    }

    console.log(`   Step 1: Fetching companies via Apollo...`);
    const apolloData = await apolloPost("/mixed_companies/search", apolloPayload) as any;
    const companies = apolloData.organizations || [];
    console.log(`   Found ${companies.length} companies.`);

    // ── Phase 2: Format Data (Ready for On-Demand Frontend) ──────────────────
    const rawRows: any[] = [];
    const cleanRows: any[] = [];

    companies.forEach((company: any) => {
      const domain = canonicalDomain(company.website_url || company.primary_domain);
      if (!domain || !company.website_url) return;

      const rawId = crypto.randomUUID();
      const phone = company.sanitized_phone || company.primary_phone?.number || null;

      // 🥉 BRONZE
      rawRows.push({
        id: rawId,
        source: "apollo",
        search_query: search_query,
        domain: domain,
        raw_payload: company
      });

      // 🥈 SILVER
      cleanRows.push({
        raw_prospect_id: rawId,
        preference_id: preference_id || null,
        search_query: search_query,
        domain: domain,
        company_name: company.name,
        phone: phone,
        address: company.raw_address || [company.street_address, company.city, company.state].filter(Boolean).join(", ") || null,
        listing_url: company.linkedin_url || null,
        source: "apollo",
        business_type: "Established Company",
        lead_tier: phone ? "tier_2_phone" : "tier_3_none",
        pitch_angle: "Pending Analysis",
        has_website: true,
        status: "pending", 
        key_people: [],
        people_scan_status: "pending", // 🎯 Tells frontend to show "Find People" button
        tech_scan_status: "pending",   // 🎯 Tells frontend to show "Scan Tech Stack" button
        tech_stack: [],
        lead_data: {
          founded_year: company.founded_year,
          alexa_ranking: company.alexa_ranking,
          logo_url: company.logo_url,
          twitter_url: company.twitter_url,
          employee_count: company.estimated_num_employees || company.num_employees || null,
          industry: company.industry
        }
      });
    });

    // ── Phase 3: Database Upserts ────────────────────────────────────────────
    console.log(`\n   💾 Saving ${cleanRows.length} prospects...`);

    if (cleanRows.length > 0) {
      // 1. Raw Payload
      await supabase.from("raw_prospects").insert(rawRows);

      // 2. Clean Prospects
      const { data: inserted, error: prospectsErr } = await supabase
        .from("prospects")
        .upsert(cleanRows, { onConflict: "domain" })
        .select("id");
      if (prospectsErr) throw new Error(`Prospects DB Error: ${prospectsErr.message}`);

      // 3. User Link
      if (inserted && inserted.length > 0 && user_id) {
        await supabase.from("user_prospects").upsert(
          inserted.map((p: { id: string }) => ({ user_id, prospect_id: p.id })),
          { onConflict: "user_id,prospect_id" }
        );
      }
    }

    // ── Phase 4: Complete Job ────────────────────────────────────────────
    if (preference_id) {
      // ✅ 1. Mark the job as completed in the database
      await supabase.from("user_preferences")
        .update({ status: "completed", completed_at: new Date().toISOString() })
        .eq("id", preference_id);
      
      // ✅ 2. The function ends here. No background tasks are fired.
      console.log(`   🏁 Job completed. Awaiting on-demand requests from frontend.`);
    }

    return new Response(
      JSON.stringify({ success: true, saved_count: cleanRows.length }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );

  } catch (error) {
    const msg = (error as Error).message;
    console.error(`⛔ [APOLLO-TECH-SCANNER CRASH] ${msg}`);
    try {
      const { preference_id } = await req.clone().json();
      if (preference_id) {
        await createClient(supabaseUrl, supabaseServiceKey)
          .from("user_preferences")
          .update({ status: "failed", error_message: msg })
          .eq("id", preference_id);
      }
    } catch { /* ignore secondary crash */ }
    return new Response(
      JSON.stringify({ error: msg }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
});