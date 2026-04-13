// ═══════════════════════════════════════════════════════════════════════════════
// EDGE FUNCTION: detect-tech-stack (ON-DEMAND VERSION)
// ═══════════════════════════════════════════════════════════════════════════════

import { serve }        from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const RAPIDAPI_KEY       = Deno.env.get("RAPIDAPI_KEY")!;

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    const { prospect_id, domain } = await req.json();

    if (!prospect_id || !domain) {
      throw new Error("Missing prospect_id or domain");
    }

    // ✅ Strip any protocol prefix — API expects bare domain e.g. "github.com"
    const cleanDomain = domain.replace(/^https?:\/\//, "").replace(/\/$/, "");

    console.log(`\n🚀 [ON-DEMAND SCAN] Scanning: ${cleanDomain}`);

    // ── 1. Call DetectZeStack ───────────────────────────────────────────────
    const apiRes = await fetch(
      `https://detectzestack.p.rapidapi.com/analyze?url=${encodeURIComponent(cleanDomain)}`,
      {
        method: "GET",
        headers: {
          "X-RapidAPI-Key":  RAPIDAPI_KEY,
          "X-RapidAPI-Host": "detectzestack.p.rapidapi.com",
        },
      }
    );

    if (!apiRes.ok) {
      const errText = await apiRes.text();
      throw new Error(`RapidAPI Error: ${apiRes.status} - ${errText}`);
    }

    const apiData = await apiRes.json();

    // ── 2. Extract only technology names ───────────────────────────────────
    const rawTech: any[] = apiData.technologies || [];
    const cleanTechStack: string[] = rawTech.map((tech) => tech.name).filter(Boolean);

    console.log(`   ✅ Found ${cleanTechStack.length} technologies.`);

    // ── 3. Update the prospects table ──────────────────────────────────────
    const { error: updateErr } = await supabase
      .from("prospects")
      .update({ tech_stack: cleanTechStack })
      .eq("id", prospect_id);

    if (updateErr) throw updateErr;

    // ── 4. Return to frontend ───────────────────────────────────────────────
    return new Response(
      JSON.stringify({ success: true, tech_stack: cleanTechStack }),
      { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );

  } catch (error) {
    const msg = (error as Error).message;
    console.error(`⛔ [SCAN CRASH] ${msg}`);
    return new Response(
      JSON.stringify({ error: msg }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }
});