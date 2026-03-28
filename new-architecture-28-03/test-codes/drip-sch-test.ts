// ═══════════════════════════════════════════════════════════════════════════════
// EDGE FUNCTION: drip-scheduler
//
// FILE: supabase/functions/drip-scheduler/index.ts
// DEPLOY: supabase functions deploy drip-scheduler
//
// CALLED BY: pg_cron every night at midnight (see task17.sql)
//            OR manually to test: POST /functions/v1/drip-scheduler with body {}
//
// WHAT IT DOES:
//   1. Loads all users with is_monthly_drip = true
//   2. Checks each user hasn't hit 1000/month limit
//   3. Picks next niche from MASTER_NICHES array using drip_niche_index
//   4. Calls cold-leads-drip for 33 leads
//   5. Advances drip_niche_index (cycles back to 0 when exhausted)
// ═══════════════════════════════════════════════════════════════════════════════

import { serve }        from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl        = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const LEADS_PER_DAY   = 33;   // 33/day × 30 days = ~1000/month
const LEADS_PER_MONTH = 1000; // hard monthly cap

// ── MASTER NICHES — 50 Google Maps business types for web design agencies ─────
// We keep the full list here so you don't lose it!
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

// ═════════════════════════════════════════════════════════════════════════════
serve(async (req) => {
  const supabase = createClient(supabaseUrl, supabaseServiceKey);

  try {
    console.log(`\n${"═".repeat(60)}`);
    console.log(`⏰ [DRIP-SCHEDULER] Starting nightly run`);
    console.log(`   Time: ${new Date().toISOString()}`);
    console.log(`${"═".repeat(60)}`);

    // ── Load all drip-enabled users ───────────────────────────────────────────
    const { data: users, error: fetchErr } = await supabase
      .from("profiles")
      .select("id, drip_location, drip_niche_index, drip_leads_this_month, name, email")
      .eq("is_monthly_drip", true);

    if (fetchErr) throw new Error(`Failed to load drip users: ${fetchErr.message}`);

    if (!users || users.length === 0) {
      console.log("   No users with drip enabled.");
      return new Response(
        JSON.stringify({ success: true, processed: 0, message: "No drip users" }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    console.log(`\n   Found ${users.length} drip-enabled users:`);
    users.forEach((u) =>
      console.log(`   • ${u.name || u.email || u.id} — ${u.drip_location} — ${u.drip_leads_this_month || 0}/1000 this month`)
    );

    let totalSaved   = 0;
    let totalSkipped = 0;

    for (const user of users) {
      const thisMonthCount = user.drip_leads_this_month || 0;
      
      // Default to Bangalore if user left it blank
      const location       = user.drip_location || "Bangalore, India";
      
      const nicheIndex     = user.drip_niche_index || 0;
      
      // 🛑 TESTING OVERRIDE: 
      // Instead of pulling from the array, we force it to "restaurant" right here:
      const niche          = "restaurant"; 

      console.log(`\n   ${"─".repeat(50)}`);
      console.log(`   👤 User: ${user.name || user.id}`);
      console.log(`   Location: ${location}`);
      console.log(`   Tonight's niche: "${niche}" (Forced for testing)`);
      console.log(`   This month: ${thisMonthCount} / ${LEADS_PER_MONTH}`);

      // Skip if monthly cap reached
      if (thisMonthCount >= LEADS_PER_MONTH) {
        console.log(`   ⏭️  Monthly cap reached — skipping`);
        continue;
      }

      // Don't overshoot the monthly cap
      const target = Math.min(LEADS_PER_DAY, LEADS_PER_MONTH - thisMonthCount);

      // ── Call cold-leads-drip ────────────────────────────────────────────────
      try {
        const dripRes = await fetch(`${supabaseUrl}/functions/v1/cold-leads-drip`, {
          method:  "POST",
          headers: {
            "Content-Type":  "application/json",
            "Authorization": `Bearer ${supabaseServiceKey}`,
          },
          body: JSON.stringify({
            user_id:      user.id,
            location,
            niche,
            target_count: target,
          }),
        });

        const dripData = await dripRes.json() as any;

        if (dripRes.ok && dripData.success) {
          totalSaved   += dripData.saved_count   || 0;
          totalSkipped += dripData.skipped_count || 0;
          console.log(`   ✅ Saved: ${dripData.saved_count} cold leads | Skipped: ${dripData.skipped_count}`);
        } else {
          console.error(`   ❌ cold-leads-drip failed: ${dripData.error || "unknown"}`);
        }
      } catch (e) {
        console.error(`   ❌ cold-leads-drip call error: ${(e as Error).message}`);
      }

      // ── Advance niche index ─────────────────────────────────────────────────
      const nextIndex = (nicheIndex + 1) % MASTER_NICHES.length;
      
      await supabase
        .from("profiles")
        .update({ drip_niche_index: nextIndex })
        .eq("id", user.id);

      console.log(`   → Index advanced to ${nextIndex} for tomorrow`);
    }

    console.log(`\n${"═".repeat(60)}`);
    console.log(`🏁 [DRIP-SCHEDULER] Nightly run complete`);
    console.log(`   Users processed: ${users.length}`);
    console.log(`   Total saved:     ${totalSaved}`);
    console.log(`   Total skipped:   ${totalSkipped}`);
    console.log(`${"═".repeat(60)}\n`);

    return new Response(
      JSON.stringify({
        success:    true,
        processed:  users.length,
        saved:      totalSaved,
        skipped:    totalSkipped,
      }),
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