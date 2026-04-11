// ═══════════════════════════════════════════════════════════════════════════════
// EDGE FUNCTION 2: apify-webhook
//
// FILE: supabase/functions/apify-webhook/index.ts
//
// ⚠️  DEPLOY WITH: supabase functions deploy apify-webhook --no-verify-jwt
// ═══════════════════════════════════════════════════════════════════════════════

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const APIFY_API_TOKEN    = Deno.env.get("APIFY_API_TOKEN")!;

const FINAL_OUTPUT_SIZE = 5;

const SOCIAL_DOMAINS = [
  "facebook.com","fb.com","instagram.com","twitter.com","x.com",
  "linkedin.com","youtube.com","tiktok.com","pinterest.com",
];
const DIR_DOMAINS = [
  "justdial.com","sulekha.com","zomato.com","swiggy.com","tripadvisor.",
  "yelp.","indiamart.com","magicpin.in","yellowpages.","lbb.in",
];

function safeHostname(raw: string): string | null {
  try { return new URL(raw).hostname.replace("www.", ""); } catch { return null; }
}
function isSocial(url: string): boolean {
  if (!url) return false;
  const h = safeHostname(url);
  return h ? SOCIAL_DOMAINS.some((d) => h.includes(d)) : false;
}
function isRealWebsite(url: string): boolean {
  if (!url) return false;
  const h = safeHostname(url);
  if (!h) return false;
  if (DIR_DOMAINS.some((d) => h.includes(d))) return false;
  if (isSocial(url)) return false;
  return true;
}
function classifyTier(website: string, socials: string[]) {
  if (isRealWebsite(website)) return { tier: "tier_1_website", tierLabel: "Has Website" };
  if (socials.filter(isSocial).length > 0) return { tier: "tier_2_social_media", tierLabel: "Has Social Media" };
  return { tier: "tier_3_none", tierLabel: "No Online Presence" };
}
const TIER_BONUS: Record<string, number> = {
  "tier_1_website":      50,
  "tier_2_social_media": 25,
  "tier_3_none":          0,
};

serve(async (req) => {
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    const url          = new URL(req.url);
    const pref_id      = url.searchParams.get("pref_id") || "";
    const user_id      = url.searchParams.get("user_id") || "";
    const search_query = url.searchParams.get("search_query") || "";

    const payload = await req.json();

    console.log(`\n📡 [APIFY-WEBHOOK] Raw payload keys: ${Object.keys(payload).join(", ")}`);
    console.log(`   resource keys: ${Object.keys(payload.resource || {}).join(", ")}`);

    const resource   = payload.resource   || {};
    const eventData  = payload.eventData  || {};
    const run_id     = resource.id        || eventData.actorRunId || "";
    const status     = resource.status    || eventData.status     || "";
    const dataset_id = resource.defaultDatasetId || eventData.defaultDatasetId || "";

    console.log(`   Run: ${run_id} | Status: ${status}`);
    console.log(`   pref_id: ${pref_id} | user_id: ${user_id}`);
    console.log(`   dataset_id: ${dataset_id}`);

    if (!pref_id || !user_id) {
      throw new Error(`Missing URL params — pref_id: "${pref_id}", user_id: "${user_id}"`);
    }

    if (status !== "SUCCEEDED") {
      console.error(`   Apify run did not succeed: "${status}"`);
      await supabase.from("lead_preferences")
        .update({ status: "failed", error_message: `Apify run ${status}` })
        .eq("id", pref_id);
      return new Response(
        JSON.stringify({ success: false, status }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    if (!dataset_id) {
      throw new Error("No dataset_id found in Apify payload");
    }

    const dataRes = await fetch(
      `https://api.apify.com/v2/datasets/${dataset_id}/items?token=${APIFY_API_TOKEN}&limit=120`
    );
    if (!dataRes.ok) throw new Error(`Dataset fetch failed: ${dataRes.status}`);
    const places = await dataRes.json();
    console.log(`   📍 ${places.length} places received from Apify.`);

    if (places.length === 0) {
      await supabase.from("lead_preferences")
        .update({ status: "completed", completed_at: new Date().toISOString() })
        .eq("id", pref_id);
      return new Response(
        JSON.stringify({ success: true, leads_saved: 0 }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    interface LeadEntry { lead: Record<string, any>; score: number; tier: string; }
    const allLeads: LeadEntry[] = [];
    const seenNames = new Set<string>();

    for (const place of places) {
      const name = place.title?.trim();
      if (!name) continue;

      const norm = name.toLowerCase().replace(/[^a-z0-9\s]/g, "").replace(/\s+/g, " ").trim();
      if (seenNames.has(norm)) continue;
      seenNames.add(norm);

      const website = place.website?.trim() || "";
      const phone   = place.phone?.trim() || place.contactInfo?.phones?.[0] || "";
      const email   = place.contactInfo?.emails?.[0]?.trim() || "";
      const address = place.address?.trim() || "";

      const sp = place.socialMediaProfiles || {};
      const instagram_url = place.instagram || sp.instagram || sp.instagramUrl || "";
      const facebook_url  = place.facebook  || sp.facebook  || sp.facebookUrl  || "";
      const twitter_url   = place.twitter   || sp.twitter   || sp.twitterUrl   || "";
      const youtube_url   = place.youtube   || sp.youtube   || sp.youtubeUrl   || "";
      const linkedin_url  = place.linkedin  || sp.linkedin  || sp.linkedinUrl  || "";

      const socialMediaLinks = [
        instagram_url, facebook_url, twitter_url, youtube_url, linkedin_url,
      ].filter((u: string) => u && isSocial(u));

      const listing_url = place.url || "";
      const { tier, tierLabel } = classifyTier(website, socialMediaLinks);
      const best_link = listing_url || website || instagram_url || facebook_url
        || twitter_url || youtube_url || linkedin_url || "";

      const slug = norm.replace(/\s+/g, "-").substring(0, 60);
      const domainKey = (website && isRealWebsite(website))
        ? safeHostname(website)!
        : place.placeId
          ? `maps#${place.placeId}`
          : `maps#${slug}`;

      const baseScore =
        (name        ? 10 : 0) +
        (phone       ? 30 : 0) +
        (email       ? 30 : 0) +
        (address     ? 10 : 0) +
        (listing_url ?  8 : 0) +
        (place.categoryName ? 5 : 0);

      const reviewBonus = Math.min(10, Math.floor((place.reviewsCount || 0) / 10));
      const ratingBonus = (place.totalScore || 0) >= 4.0 ? 5 : 0;
      const totalScore  = baseScore + (TIER_BONUS[tier] ?? 0) + reviewBonus + ratingBonus;

      allLeads.push({
        score: totalScore,
        tier,
        lead: {
          preference_id: pref_id,
          search_query,
          domain: domainKey,
          source: "google_maps",
          lead_data: {
            company_name:       name,
            address,
            phone,
            email,
            website,
            listing_url,
            source_url:         place.url || "",
            best_link,
            instagram_url,
            facebook_url,
            twitter_url,
            youtube_url,
            other_social_url:   linkedin_url,
            social_media_links: socialMediaLinks,
            tier,
            tier_label:         tierLabel,
            description:        place.categoryName || "",
            google_maps_url:    place.url || "",
            google_place_id:    place.placeId || "",
            rating:             place.totalScore || null,
            review_count:       place.reviewsCount || 0,
            category:           place.categoryName || "",
            neighborhood:       place.neighborhood || "",
            latitude:           place.location?.lat || null,
            longitude:          place.location?.lng || null,
            opening_hours:      place.openingHours || null,
            price_level:        place.price || null,
            image_url:          place.imageUrls?.[0] || place.imageUrl || "",
            temporarily_closed: place.temporarilyClosed || false,
            permanently_closed: place.permanentlyClosed || false,
          },
          status: "verified",
        },
      });
    }

    allLeads.sort((a, b) => {
      if (a.tier !== b.tier) return a.tier.localeCompare(b.tier);
      return b.score - a.score;
    });

    const finalLeads   = allLeads.slice(0, FINAL_OUTPUT_SIZE).map((x) => x.lead);
    const domainSeen   = new Set<string>();
    const dedupedLeads = finalLeads.filter((lead: any) => {
      if (domainSeen.has(lead.domain)) return false;
      domainSeen.add(lead.domain);
      return true;
    });

    console.log(`   📦 ${allLeads.length} processed → ${dedupedLeads.length} after dedup.`);

    if (dedupedLeads.length > 0) {
      const { error: leadsErr } = await supabase
        .from("leads")
        .upsert(dedupedLeads, { onConflict: "domain" });

      if (leadsErr) {
        console.error("   ❌ Leads upsert error:", leadsErr.message);
      } else {
        console.log(`   ✅ ${dedupedLeads.length} leads saved successfully.`);
      }
    }

    await supabase.from("lead_preferences")
      .update({ status: "completed", completed_at: new Date().toISOString() })
      .eq("id", pref_id);

    console.log(`   ✅ Job ${pref_id} marked completed.`);

    return new Response(
      JSON.stringify({ success: true, leads_saved: dedupedLeads.length }),
      { status: 200, headers: { "Content-Type": "application/json" } },
    );

  } catch (error) {
    console.error("⛔ [APIFY-WEBHOOK]", (error as Error).message);
    return new Response(
      JSON.stringify({ error: (error as Error).message }),
      { status: 500, headers: { "Content-Type": "application/json" } },
    );
  }
});