// ═══════════════════════════════════════════════════════════════════════════════
// EDGE FUNCTION: cold-leads-drip
//
// FILE: supabase/functions/cold-leads-drip/index.ts
// DEPLOY: supabase functions deploy cold-leads-drip
// ═══════════════════════════════════════════════════════════════════════════════

import { serve }        from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const APIFY_API_TOKEN    = Deno.env.get("APIFY_API_TOKEN")!;

const ACTOR_ID     = "compass/crawler-google-places";
const ACTOR_URL_ID = ACTOR_ID.replace("/", "~");

const NEWBORN_REVIEW_THRESHOLD = 3;

function classifyPlace(place: any): {
  has_website:   boolean;
  is_newborn:    boolean;
  include:       boolean; 
  skip_reason:   string | null;
} {
  const website     = (place.website || "").trim();
  const reviewCount = place.reviewsCount ?? place.totalReviews ?? 0;

  const has_website = website.length > 0;
  const is_newborn  = !has_website && reviewCount <= NEWBORN_REVIEW_THRESHOLD;

  if (has_website) {
    return { has_website: true, is_newborn: false, include: false, skip_reason: "has website" };
  }

  return { has_website: false, is_newborn, include: true, skip_reason: null };
}

async function runApifyAndWait(
  query: string,
  maxResults: number
): Promise<any[]> {
  const runRes = await fetch(
    `https://api.apify.com/v2/acts/${ACTOR_URL_ID}/runs?token=${APIFY_API_TOKEN}`,
    {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        searchStringsArray:        [query],
        maxCrawledPlacesPerSearch: 10, // Keeping at 10 for testing
        language:                  "en",
        
        // 🚀 THE EXACT APIFY FILTER YOU FOUND:
        website:                   "withoutWebsite",
        
        skipClosedPlaces:          true, 
        maxReviews:                0,
        scrapeSocialMediaProfiles: { facebooks: false, instagrams: false, youtubes: false, tiktoks: false, twitters: false },
        maximumLeadsEnrichmentRecords: 0,
      }),
    }
  );

  if (!runRes.ok) throw new Error(`Apify start failed: ${runRes.status} ${await runRes.text()}`);

  const { data: runData } = await runRes.json();
  const runId      = runData.id;
  const datasetId  = runData.defaultDatasetId;

  console.log(`   Apify run started: ${runId}`);

  const maxWait = 180_000;
  const poll    = 5_000;
  const start   = Date.now();

  while (Date.now() - start < maxWait) {
    await new Promise((r) => setTimeout(r, poll));

    const statusRes = await fetch(
      `https://api.apify.com/v2/actor-runs/${runId}?token=${APIFY_API_TOKEN}`
    );
    const { data: status } = await statusRes.json();

    console.log(`   Apify status: ${status.status} (${Math.round((Date.now() - start) / 1000)}s)`);

    if (status.status === "SUCCEEDED") break;
    if (["FAILED", "ABORTED", "TIMED-OUT"].includes(status.status)) {
      throw new Error(`Apify run ${status.status}`);
    }
  }

  const dataRes = await fetch(
    `https://api.apify.com/v2/datasets/${datasetId}/items?token=${APIFY_API_TOKEN}&limit=120`
  );
  if (!dataRes.ok) throw new Error(`Dataset fetch failed: ${dataRes.status}`);

  return dataRes.json();
}

function canonicalDomain(raw: string | null | undefined): string | null {
  if (!raw) return null;
  const trimmed = raw.trim();
  const withProtocol = trimmed.startsWith("http") ? trimmed : `https://${trimmed}`;
  try {
    let hostname = new URL(withProtocol).hostname.toLowerCase();
    hostname = hostname.replace(/^www\d*\./, "");
    if (hostname.includes(".")) return hostname;
  } catch {}
  return null;
}

serve(async (req) => {
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    const { user_id, location = "Bangalore, India", niche, target_count = 33 } = await req.json();

    if (!user_id || !niche) {
      throw new Error("Missing required fields: user_id, niche");
    }

    const query = `${niche} in ${location}`;
    console.log(`\n🏗️  [COLD-LEADS-DRIP]`);
    console.log(`   user_id:      ${user_id}`);
    console.log(`   query:        "${query}"`);
    console.log(`   target_count: ${target_count}`);

    console.log(`\n   Step 1: Fetching Google Maps results…`);
    const places = await runApifyAndWait(query, target_count);
    console.log(`   Got ${places.length} places from Apify (No-Website filter applied!)`);

    console.log(`\n   Step 2: Classifying leads…`);

    const coldLeads:  any[] = [];
    let skippedCount = 0;
    const seenDomains = new Set<string>();

    for (const place of places) {
      const name = (place.title || "").trim();
      if (!name) continue;

      const { has_website, is_newborn, include, skip_reason } = classifyPlace(place);

      const website = (place.website || "").trim();
      const domain  = canonicalDomain(website || place.url);

      const dedupKey = domain || name.toLowerCase().replace(/[^a-z0-9]/g, "").slice(0, 40);
      if (seenDomains.has(dedupKey)) continue;
      seenDomains.add(dedupKey);

      const reviewCount = place.reviewsCount ?? 0;

      console.log(`   ${include ? "✅" : "⏭️ "} ${name} | website=${has_website} | reviews=${reviewCount}${skip_reason ? ` | skip: ${skip_reason}` : is_newborn ? " | NEWBORN" : ""}`);

      if (!include) {
        skippedCount++;
        continue;
      }

      if (coldLeads.length >= target_count) break;

      const phone   = place.phone || place.contactInfo?.phones?.[0] || "";
      const address = place.address || "";

      coldLeads.push({
        preference_id: null,         
        search_query:  query,
        domain:        domain || `maps#${place.placeId || dedupKey}`,
        source:        "google_maps",
        drip_source:   "cold_drip",
        has_website,
        review_count:  reviewCount,
        status:        "verified",
        lead_data: {
          company_name:  name,
          address,
          phone,
          email:         place.contactInfo?.emails?.[0] || "",
          website:       website || null,
          listing_url:   place.url || "",
          google_maps_url: place.url || "",
          google_place_id: place.placeId || "",
          rating:          place.totalScore || null,
          review_count:    reviewCount,
          category:        place.categoryName || niche,
          has_website,
          is_newborn_business: is_newborn,
          approach: is_newborn ? "ii_newborn" : "i_null_website",
          tier:     "tier_3_none",
          tier_label: "No online presence",
          source:   "cold_drip",
          location: address || location,
          niche,
        },
      });
    }

    console.log(`\n   Classification summary:`);
    console.log(`   Cold leads (no website): ${coldLeads.length}`);
    console.log(`   Skipped (safety net caught website):   ${skippedCount}`);
    console.log(`   Newborn businesses:      ${coldLeads.filter((l) => l.lead_data.is_newborn_business).length}`);

    if (coldLeads.length === 0) {
      console.log(`   ⚠️  No cold leads found for "${query}"`);
      return new Response(
        JSON.stringify({ success: true, saved_count: 0, skipped_count: skippedCount }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    console.log(`\n   Step 3: Saving ${coldLeads.length} leads…`);

    const { data: inserted, error: leadsErr } = await supabase
      .from("leads")
      .upsert(coldLeads, { onConflict: "domain" })
      .select("id");

    if (leadsErr) throw new Error(`leads upsert: ${leadsErr.message}`);

    if (inserted && inserted.length > 0) {
      const junction = inserted.map((l: { id: string }) => ({
        user_id,
        lead_id: l.id,
      }));

      const { error: jErr } = await supabase
        .from("user_leads")
        .upsert(junction, { onConflict: "user_id,lead_id" });

      if (jErr) console.error(`   user_leads upsert error: ${jErr.message}`);
      else      console.log(`   ✅ ${inserted.length} leads linked to user`);
    }

    const { data: profile } = await supabase
      .from("profiles")
      .select("drip_leads_this_month")
      .eq("id", user_id)
      .single();

    if (profile) {
      await supabase
        .from("profiles")
        .update({ drip_leads_this_month: (profile.drip_leads_this_month || 0) + coldLeads.length })
        .eq("id", user_id);
    }

    console.log(`\n   🏁 [COLD-LEADS-DRIP] Done`);
    console.log(`   Saved: ${coldLeads.length} | Skipped: ${skippedCount}`);

    return new Response(
      JSON.stringify({
        success:       true,
        saved_count:   coldLeads.length,
        skipped_count: skippedCount,
        query,
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );

  } catch (error) {
    const msg = (error as Error).message;
    console.error(`⛔ [COLD-LEADS-DRIP CRASH] ${msg}`);
    return new Response(
      JSON.stringify({ error: msg }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
});
