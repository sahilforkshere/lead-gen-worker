import { serve }        from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const APIFY_API_TOKEN    = Deno.env.get("APIFY_API_TOKEN")!;
const ACTOR_URL_ID       = "compass~crawler-google-places";

function classifyPlace(place: any) {
  const website = (place.website || "").trim();
  return { has_website: website.length > 0, include: website.length === 0 };
}

function canonicalDomain(raw: string | null | undefined): string | null {
  if (!raw) return null;
  const s = raw.trim().startsWith("http") ? raw.trim() : `https://${raw.trim()}`;
  try {
    let h = new URL(s).hostname.toLowerCase().replace(/^www\d*\./, "");
    return h.includes(".") ? h : null;
  } catch { return null; }
}

async function runApifyAndWait(searchString: string, location: string, maxResults: number): Promise<any[]> {
  const runRes = await fetch(
    `https://api.apify.com/v2/acts/${ACTOR_URL_ID}/runs?token=${APIFY_API_TOKEN}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        searchStringsArray:        [searchString],
        locationQuery:             location,
        maxCrawledPlacesPerSearch: Math.min(maxResults * 2, 20),
        language:                  "en",
        website:                   "withoutWebsite",
        skipClosedPlaces:          true,
        maxReviews:                0,
        scrapeSocialMediaProfiles: { facebooks:false, instagrams:false, youtubes:false, tiktoks:false, twitters:false },
        maximumLeadsEnrichmentRecords: 0,
      }),
    }
  );
  if (!runRes.ok) throw new Error(`Apify start failed: ${runRes.status}`);

  const { data: runData } = await runRes.json();
  const { id: runId, defaultDatasetId: datasetId } = runData;
  console.log(`   Apify run: ${runId}`);

  const start = Date.now();
  while (Date.now() - start < 180_000) {
    await new Promise(r => setTimeout(r, 5_000));
    const { data: status } = await (await fetch(`https://api.apify.com/v2/actor-runs/${runId}?token=${APIFY_API_TOKEN}`)).json();
    console.log(`   Apify: ${status.status} (${Math.round((Date.now()-start)/1000)}s)`);
    if (status.status === "SUCCEEDED") break;
    if (["FAILED","ABORTED","TIMED-OUT"].includes(status.status)) throw new Error(`Apify ${status.status}`);
  }

  const dataRes = await fetch(`https://api.apify.com/v2/datasets/${datasetId}/items?token=${APIFY_API_TOKEN}&limit=20`);
  if (!dataRes.ok) throw new Error(`Dataset fetch failed: ${dataRes.status}`);
  return dataRes.json();
}

serve(async (req) => {
  const supabase = createClient(supabaseUrl, supabaseServiceKey);
  try {
    const {
      user_id,
      location     = "London",
      niche,
      search_query,
      target_count = 10,
    } = await req.json();

    if (!user_id) throw new Error("Missing user_id");

    // search_query (from settings) takes priority over niche (scheduler rotation)
    const apifySearchString = (search_query || niche || "").trim();
    if (!apifySearchString) throw new Error("No search query or niche provided");

    // Avoid "photographers in New Delhi in New Delhi"
    const locationNorm = location.trim().toLowerCase();
    const query = apifySearchString.toLowerCase().includes(locationNorm)
      ? apifySearchString
      : `${apifySearchString} in ${location}`;

    console.log(`\n🏗️  [COLD-LEADS-DRIP] query="${query}" target=${target_count}`);

    const places = await runApifyAndWait(apifySearchString, location, target_count);
    console.log(`   Got ${places.length} places from Apify`);

    const rawRows: any[]   = [];
    const cleanRows: any[] = [];
    let skippedCount = 0;
    const seenKeys = new Set<string>();

    for (const place of places) {
      const name = (place.title || "").trim();
      if (!name) continue;

      const { has_website, include } = classifyPlace(place);
      if (!include) { skippedCount++; continue; }
      if (cleanRows.length >= target_count) break;

      const placeId        = place.placeId || "";
      const website        = (place.website || "").trim();
      const permanentUrl   = placeId
        ? `https://www.google.com/maps/place/?q=place_id:${placeId}`
        : (place.url || "");
      const finalDomain    = canonicalDomain(website)
        || permanentUrl
        || `maps#${name.toLowerCase().replace(/[^a-z0-9]/g,"").slice(0,40)}`;

      if (seenKeys.has(finalDomain)) continue;
      seenKeys.add(finalDomain);

      const phone   = place.phone || place.contactInfo?.phones?.[0] || "";
      const address = place.address || "";
      const email   = place.contactInfo?.emails?.[0] || null;
      const tier    = email ? "tier_1_verified" : phone ? "tier_2_phone" : "tier_3_none";

      console.log(`   ✅ ${name} | reviews=${place.reviewsCount ?? 0} | tier=${tier}`);

      const rawId = crypto.randomUUID();
      rawRows.push({ id:rawId, source:"google_maps", search_query:query, domain:finalDomain, place_id:placeId||null, raw_payload:place });
      cleanRows.push({
        raw_prospect_id: rawId,
        search_query:    query,
        domain:          finalDomain,
        company_name:    name,
        phone,
        email,
        address,
        listing_url:     permanentUrl,
        place_id:        placeId || null,
        source:          "google_maps",
        business_type:   "Local Business",
        lead_tier:       tier,
        pitch_angle:     "Local business with no website — ideal web design prospect",
        has_website:     false,
        rating:          place.totalScore || null,
        review_count:    place.reviewsCount ?? 0,
        status:          "pending",
      });
    }

    console.log(`   Saved: ${cleanRows.length} | Skipped: ${skippedCount}`);
    if (cleanRows.length === 0)
      return new Response(JSON.stringify({ success:true, saved_count:0, skipped_count:skippedCount }), { status:200 });

    // Bronze
    if (rawRows.length > 0) {
      const { error } = await supabase.from("raw_prospects").insert(rawRows);
      if (error) console.error(`   raw_prospects error: ${error.message}`);
    }

    // Silver
    const { data: inserted, error: prospectsErr } = await supabase
      .from("prospects").upsert(cleanRows, { onConflict:"domain" }).select("id");
    if (prospectsErr) throw new Error(prospectsErr.message);

    // Gold
    if (inserted?.length) {
      const { error: jErr } = await supabase.from("user_prospects")
        .upsert(inserted.map(p => ({ user_id, prospect_id: p.id })), { onConflict:"user_id,prospect_id" });
      if (jErr) console.error(`   user_prospects error: ${jErr.message}`);
    }

    // Quota
    const { data: profile } = await supabase.from("profiles").select("drip_leads_this_month").eq("id", user_id).single();
    if (profile)
      await supabase.from("profiles").update({ drip_leads_this_month: (profile.drip_leads_this_month||0) + cleanRows.length }).eq("id", user_id);

    return new Response(JSON.stringify({ success:true, saved_count:cleanRows.length, skipped_count:skippedCount, query }), { status:200 });

  } catch (error) {
    console.error(`⛔ CRASH: ${(error as Error).message}`);
    return new Response(JSON.stringify({ error:(error as Error).message }), { status:500 });
  }
});