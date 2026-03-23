
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const OPENAI_API_KEY     = Deno.env.get("OPENAI_API_KEY")!;
const APIFY_API_TOKEN    = Deno.env.get("APIFY_API_TOKEN")!;

const ACTOR_ID     = "compass/crawler-google-places";
const ACTOR_URL_ID = ACTOR_ID.replace("/", "~");
const ALL_PIPELINES = ["web_search", "google_maps", "apollo"];

// ✅ REQUIRED FOR FRONTEND TO TALK TO EDGE FUNCTION
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

serve(async (req) => {
  // ✅ HANDLE CORS PREFLIGHT REQUESTS
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders });
  }

  const supabase = createClient(supabaseUrl, supabaseServiceKey);
  let pref_id = "";

  try {
    const body = await req.json();
    console.log("📦 RAW PAYLOAD RECEIVED:", JSON.stringify(body));

    // ✅ THE PAYLOAD SNIFFER: Handles both DB Triggers and Direct API calls safely
    const isTrigger = !!body.record;
    const search_query = isTrigger ? body.record.search_query : body.search_query;
    const user_id = isTrigger ? body.record.user_id : body.user_id;
    const selected_pipeline = (isTrigger ? body.record.selected_pipeline : body.selected_pipeline) || "apollo";

    if (!search_query || !user_id) {
      throw new Error(`Missing search_query or user_id. Payload was: ${JSON.stringify(body)}`);
    }

    // ✅ Get or create the Preference ID
    if (isTrigger && body.record.id) {
      pref_id = body.record.id;
    } else {
      const { data: newJob, error: insertErr } = await supabase
        .from('lead_preferences')
        .insert({ user_id, search_query, status: 'pending', selected_pipeline })
        .select('id').single();
      if (insertErr) throw insertErr;
      pref_id = newJob.id;
    }

    console.log(`\n🧭 [CLASSIFY-AND-ROUTE] Job: ${pref_id}`);
    console.log(`   Query: "${search_query}"`);
    console.log(`   Selected Pipeline: "${selected_pipeline}"`);

    let pipeline = selected_pipeline;

    // Only use GPT if someone explicitly passes "auto"
    if (pipeline === "auto") {
      pipeline = await classifyWithGPT(search_query);
      console.log(`   GPT decision: ${pipeline}`);
    }

    // ════════════════════════════════════════════════════════
    // ROUTE A: APOLLO
    // ════════════════════════════════════════════════════════
    if (pipeline === "apollo") {
      console.log("   🔵 Routing to Apollo pipeline…");

      await supabase.from("lead_preferences")
        .update({
          status:              "processing_maps", 
          resolved_pipeline:   "apollo",
          pipeline_type:       "apollo",
          started_at:          new Date().toISOString(),
          available_pipelines: ALL_PIPELINES,
        })
        .eq("id", pref_id);

      // Fire and forget the Apollo worker
      fetch(`${supabaseUrl}/functions/v1/apollo-pipeline`, {
        method:  "POST",
        headers: {
          "Content-Type":  "application/json",
          "Authorization": `Bearer ${supabaseServiceKey}`,
        },
        body: JSON.stringify({ pref_id, user_id, search_query }),
      }).catch((e) => console.error("   Apollo pipeline error:", e.message));

      return new Response(
        JSON.stringify({ success: true, pipeline: "apollo", available_pipelines: ALL_PIPELINES }),
        { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // ════════════════════════════════════════════════════════
    // ROUTE B: GOOGLE MAPS (APIFY)
    // ════════════════════════════════════════════════════════
    if (pipeline === "google_maps") {
      console.log("   🗺️ Starting Apify Google Maps Scraper…");

      const webhookUrl = `${supabaseUrl}/functions/v1/apify-webhook`
        + `?pref_id=${encodeURIComponent(pref_id)}`
        + `&user_id=${encodeURIComponent(user_id)}`
        + `&search_query=${encodeURIComponent(search_query)}`;

      const webhookConfig = [{
        eventTypes: ["ACTOR.RUN.SUCCEEDED", "ACTOR.RUN.FAILED", "ACTOR.RUN.ABORTED"],
        requestUrl: webhookUrl,
        headersTemplate: `{"Content-Type":"application/json","Authorization":"Bearer ${supabaseServiceKey}"}`,
      }];
      
      const webhooksBase64 = btoa(unescape(encodeURIComponent(JSON.stringify(webhookConfig))));

      const runRes = await fetch(
        `https://api.apify.com/v2/acts/${ACTOR_URL_ID}/runs?token=${APIFY_API_TOKEN}&webhooks=${webhooksBase64}`,
        {
          method:  "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            searchStringsArray: [search_query],
            maxCrawledPlacesPerSearch: 120,
            language: "en",
            scrapeSocialMediaProfiles: { facebooks: false, instagrams: false, youtubes: false, tiktoks: false, twitters: false },
            maximumLeadsEnrichmentRecords: 0,
            maxReviews: 0,
            scrapeReviewerName: false,
            scrapeReviewerUrl: false,
            scrapeResponseFromOwnerText: false,
            maxAutomaticZoomOut: 3,
            skipClosedPlaces: false,
          }),
        }
      );

      if (!runRes.ok) {
        const errText = await runRes.text();
        console.error(`   Apify start failed: ${runRes.status} - ${errText}`);
        await supabase.from("lead_preferences")
          .update({ resolved_pipeline: "web_search", pipeline_type: "web_search", available_pipelines: ALL_PIPELINES })
          .eq("id", pref_id);
        return new Response(
          JSON.stringify({ success: true, pipeline: "web_search", reason: "apify_start_failed" }),
          { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
        );
      }

      const runData = await runRes.json();
      const runId   = runData.data?.id;
      console.log(`   ✅ Apify run started: ${runId}`);

      await supabase.from("lead_preferences")
        .update({
          status:              "processing_maps",
          resolved_pipeline:   "google_maps",
          pipeline_type:       "google_maps",
          apify_run_id:        runId,
          started_at:          new Date().toISOString(),
          available_pipelines: ALL_PIPELINES,
        })
        .eq("id", pref_id);

      return new Response(
        JSON.stringify({ success: true, pipeline: "google_maps", run_id: runId }),
        { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      );
    }

    // ════════════════════════════════════════════════════════
    // ROUTE C: WEB SEARCH
    // ════════════════════════════════════════════════════════
    console.log("   🔍 Routing to web_search worker.");
    await supabase.from("lead_preferences")
      .update({ resolved_pipeline: "web_search", pipeline_type: "web_search", available_pipelines: ALL_PIPELINES })
      .eq("id", pref_id);

    return new Response(
      JSON.stringify({ success: true, pipeline: "web_search" }),
      { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );

  } catch (error) {
    console.error("⛔ [CLASSIFY-AND-ROUTE] Error:", (error as Error).message);
    if (pref_id) {
      await createClient(supabaseUrl, supabaseServiceKey)
        .from("lead_preferences")
        .update({ resolved_pipeline: "web_search", pipeline_type: "web_search" })
        .eq("id", pref_id)
        .catch(() => {});
    }
    return new Response(
      JSON.stringify({ error: (error as Error).message, fallback: "web_search" }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// GPT CLASSIFIER
// ═══════════════════════════════════════════════════════════════════════════════
async function classifyWithGPT(query: string): Promise<string> {
  try {
    const res = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type":  "application/json",
        "Authorization": `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content: `Return ONLY a JSON object in this exact format: { "pipeline": "google_maps" } or { "pipeline": "web_search" }`,
          },
          { role: "user", content: `Classify: "${query}"` },
        ],
      }),
    });

    const data = await res.json();
    if (data.error || !data.choices) return "web_search";
    const result = JSON.parse(data.choices[0].message.content);
    return result.pipeline === "google_maps" ? "google_maps" : "web_search";

  } catch (e) {
    return "web_search";
  }
}