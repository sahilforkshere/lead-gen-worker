// ═══════════════════════════════════════════════════════════════════════════════
// EDGE FUNCTION: drip-scheduler
//
// FILE: supabase/functions/drip-scheduler/index.ts
// DEPLOY: supabase functions deploy drip-scheduler
// TRIGGERED BY: pg_cron every night at midnight UTC
//               OR manually: POST /functions/v1/drip-scheduler body: {}
// ═══════════════════════════════════════════════════════════════════════════════

import { serve }        from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const LEADS_PER_DAY   = 33;
const LEADS_PER_MONTH = 1000;

const MASTER_NICHES = [
  "plumber", "electrician", "roofing contractor", "hvac", "locksmith",
  "painter", "carpenter", "landscaper", "cleaning service", "pest control",
  "restaurant", "cafe", "bakery", "bar", "pizza delivery",
  "dentist", "physiotherapist", "gym", "yoga studio", "massage therapist",
  "accountant", "lawyer", "insurance agent", "real estate agent", "mortgage broker",
  "florist", "hardware store", "clothing boutique", "gift shop", "bookstore",
  "car mechanic", "car wash", "auto body shop", "tire shop", "driving school",
  "tutoring center", "music school", "dance studio", "martial arts school", "daycare",
  "wedding photographer", "event planner", "travel agent", "hotel", "bed and breakfast",
  "moving company", "security company", "printing shop", "sign maker", "storage facility",
];

// mechanism → which edge function to call
const PIPELINE_MAP: Record<string, string> = {
  no_website:   "cold-leads-drip",    // Apify Maps + website:withoutWebsite
  new_business: "cold-leads-drip",    // Apify Maps + review_count ≤ 5
  new_domain:   "new-domain-scanner", // WhoisXML + websitelaunches + Hunter.io
};

serve(async (req) => {
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    console.log(`\n${"═".repeat(60)}`);
    console.log(`⏰ [DRIP-SCHEDULER] Nightly run`);
    console.log(`   Time: ${new Date().toISOString()}`);
    console.log(`${"═".repeat(60)}`);

    // ── Load all drip-enabled users ──────────────────────────────────────────
    const { data: users, error: fetchErr } = await supabase
      .from("profiles")
      .select("id, name, email, drip_location, drip_niche_index, drip_target_niche, drip_leads_this_month, drip_mechanism")
      .eq("is_monthly_drip", true);

    if (fetchErr) {
      console.error(`❌ DB error: ${fetchErr.message}`);
      throw new Error(`Failed to load drip users: ${fetchErr.message}`);
    }

    if (!users || users.length === 0) {
      console.log("   😴 No users with drip enabled.");
      return new Response(
        JSON.stringify({ success: true, processed: 0, message: "No drip users" }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    console.log(`\n   Found ${users.length} drip-enabled users:`);
    users.forEach((u) =>
      console.log(`   • ${u.name || u.email || u.id} | ${u.drip_location} | mechanism=${u.drip_mechanism} | ${u.drip_leads_this_month || 0}/1000`)
    );

    let totalSaved = 0;

    for (const user of users) {
      const thisMonthCount = user.drip_leads_this_month || 0;
      const location       = user.drip_location  || "London";
      const mechanism      = user.drip_mechanism || "no_website";
      const nicheIndex     = user.drip_niche_index || 0;
      const savedNiche     = (user.drip_target_niche || "any").trim().toLowerCase();

      // ── Niche selection ───────────────────────────────────────────────────
      let niche: string;
      if (savedNiche === "any" || savedNiche === "") {
        niche = MASTER_NICHES[nicheIndex % MASTER_NICHES.length];
        console.log(`\n   ${"─".repeat(50)}`);
        console.log(`   👤 ${user.name || user.id}`);
        console.log(`   🎰 Roulette → index ${nicheIndex} = "${niche}"`);
      } else {
        niche = savedNiche;
        console.log(`\n   ${"─".repeat(50)}`);
        console.log(`   👤 ${user.name || user.id}`);
        console.log(`   🎯 Fixed niche = "${niche}"`);
      }

      console.log(`   📍 Location:  ${location}`);
      console.log(`   ⚙️  Mechanism: ${mechanism}`);
      console.log(`   📊 Quota:     ${thisMonthCount} / ${LEADS_PER_MONTH}`);

      if (thisMonthCount >= LEADS_PER_MONTH) {
        console.log(`   ⏭️  Monthly cap reached — skipping`);
        continue;
      }

      const target   = Math.min(LEADS_PER_DAY, LEADS_PER_MONTH - thisMonthCount);
      const targetFn = PIPELINE_MAP[mechanism] || "cold-leads-drip";
      console.log(`   🚦 Route → ${targetFn} | target: ${target} leads`);

      // ── Call pipeline ─────────────────────────────────────────────────────
      try {
        const res = await fetch(`${supabaseUrl}/functions/v1/${targetFn}`, {
          method:  "POST",
          headers: {
            "Content-Type":  "application/json",
            "Authorization": `Bearer ${supabaseServiceKey}`,
          },
          body: JSON.stringify({ user_id: user.id, location, niche, target_count: target, mechanism }),
        });

        const body = await res.json() as any;

        if (res.ok && body.success) {
          totalSaved += body.saved_count || 0;
          console.log(`   ✅ Saved: ${body.saved_count || 0} leads`);
        } else {
          console.error(`   ❌ Error from ${targetFn}: ${body.error || JSON.stringify(body).slice(0, 150)}`);
        }
      } catch (err) {
        console.error(`   ❌ Network error → ${targetFn}: ${(err as Error).message}`);
      }

      // ── Advance niche index for roulette users ────────────────────────────
      if (savedNiche === "any" || savedNiche === "") {
        const nextIndex = (nicheIndex + 1) % MASTER_NICHES.length;
        const { error: updateErr } = await supabase
          .from("profiles")
          .update({ drip_niche_index: nextIndex })
          .eq("id", user.id);

        if (updateErr) console.error(`   ❌ Index update failed: ${updateErr.message}`);
        else           console.log(`   → Next: "${MASTER_NICHES[nextIndex]}" (index ${nextIndex})`);
      }
    }

    console.log(`\n${"═".repeat(60)}`);
    console.log(`🏁 Done | users=${users.length} | saved=${totalSaved}`);
    console.log(`${"═".repeat(60)}\n`);

    return new Response(
      JSON.stringify({ success: true, processed: users.length, saved: totalSaved }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );

  } catch (error) {
    const msg = (error as Error).message;
    console.error(`⛔ [DRIP-SCHEDULER CRASH] ${msg}`);
    return new Response(
      JSON.stringify({ error: msg }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
});