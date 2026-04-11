// ═══════════════════════════════════════════════════════════════════════════════
// EDGE FUNCTION: apollo-tech-scanner
// FILE: supabase/functions/apollo-tech-scanner/index.ts
// ═══════════════════════════════════════════════════════════════════════════════

import { serve }        from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const APOLLO_API_KEY     = Deno.env.get("APOLLO_API_KEY")!;

const APOLLO_BASE   = "https://api.apollo.io/api/v1";

// ── 1. APOLLO FETCHER (From your reference code - handles 429s and 422s safely)
async function apolloPost(path: string, body: Record<string, unknown>, retries = 3): Promise<Record<string, unknown>> {
  for (let attempt = 0; attempt < retries; attempt++) {
    const res = await fetch(`${APOLLO_BASE}${path}`, {
      method:  "POST",
      // 🎯 FIX: Apollo requires X-Api-Key in the header to prevent 422/Unauthorized errors
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

// ── 2. DOMAIN & LOCATION PARSERS (From your reference code)
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

const US_STATES: Record<string, string> = { "florida": "FL", "california": "CA", "texas": "TX", "new york": "NY" /* Add more as needed */ };
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

// ── 3. CUSTOM TECH SCANNER (Analyzes HTML for old tech)
async function scanWebsiteTech(url: string): Promise<{ is_outdated: boolean; flags: string[] }> {
  try {
    const targetUrl = url.startsWith("http") ? url : `https://${url}`;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);

    const response = await fetch(targetUrl, { signal: controller.signal });
    clearTimeout(timeoutId);

    const html = (await response.text()).toLowerCase();
    const flags: string[] = [];

    if (html.includes("wp-content/themes/")) flags.push("WordPress Theme");
    if (html.includes("jquery-1.") || html.includes("jquery-2.")) flags.push("Outdated jQuery");
    if (html.includes("<table") && html.includes("width=")) flags.push("Table-based Layout");
    if (!html.includes("viewport")) flags.push("Not Mobile Responsive");

    return { is_outdated: flags.length > 0, flags: flags };
  } catch (error) {
    return { is_outdated: true, flags: ["Site Down or Extremely Slow"] };
  }
}

// ── 4. MAIN PIPELINE ─────────────────────────────────────────────────────────
serve(async (req) => {
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    const {
      user_id,
      search_query,      // e.g., "accounting"
      location,          // e.g., "Miami"
      filters = {},      // e.g., { employee_range: ["11,50"] }
      preference_id,
      mechanism
    } = await req.json();

    console.log(`\n🚀 [APOLLO-TECH-SCANNER] Executing...`);

    // ── Phase 1: Build Safe Apollo Payload ───────────────────────────────────
    // 🎯 FIX: Dynamically building the payload prevents Apollo from throwing 422s on empty data
    const apolloPayload: Record<string, any> = {
      page: 1,
      per_page: 15
    };

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
    console.log(`   Found ${companies.length} companies to scan.`);

    // ── Phase 2: Tech Scanning ───────────────────────────────────────────────
    const rawRows: any[] = [];
    const cleanRows: any[] = [];

    const scanPromises = companies.map(async (company: any) => {
      const domain = canonicalDomain(company.website_url || company.primary_domain);
      if (!domain || !company.website_url) return;

      const techData = await scanWebsiteTech(company.website_url);
      
      console.log(`   Scanned ${company.name} | Outdated? ${techData.is_outdated ? 'YES 🚨' : 'NO ✅'}`);

      if (techData.is_outdated) {
        const rawId = crypto.randomUUID();
        const phone = company.sanitized_phone || company.primary_phone?.number || null;

        // 🥉 BRONZE
        rawRows.push({
          id: rawId,
          source: "apollo_tech",
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
          pitch_angle: `Outdated Website: ${techData.flags.join(", ")}`,
          has_website: true,
          status: "pending",
          lead_data: {
            founded_year: company.founded_year,
            alexa_ranking: company.alexa_ranking,
            logo_url: company.logo_url,
            twitter_url: company.twitter_url,
            employee_count: company.estimated_num_employees || company.num_employees || null,
            industry: company.industry
          }
        });
      }
    });

    await Promise.all(scanPromises);

    // ── Phase 3: Database Upserts ────────────────────────────────────────────
    console.log(`\n   💾 Saving ${cleanRows.length} qualified leads...`);

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

    // ── Phase 4: Complete Job ────────────────────────────────────────────────
    if (preference_id) {
      await supabase.from("user_preferences")
        .update({ status: "completed", completed_at: new Date().toISOString() })
        .eq("id", preference_id);
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