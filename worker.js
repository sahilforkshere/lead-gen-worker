import { createClient } from '@supabase/supabase-js';
import * as dotenv from 'dotenv';

dotenv.config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ─── THE HEAVY LIFTING PIPELINE ───────────────────────────────────
async function executeExtractionPipeline(job) {
  console.log(`\n⚙️ Processing Search: "${job.search_query}"`);
  
  try {
    // -> THIS IS WHERE YOUR SCRAPING MAGIC GOES <-
    // Phase 1: GPT Expansion
    // Phase 2: Parallel Discovery
    // Phase 3 & 4: Jina AI & Serper Extraction
    
    // For now, let's simulate the pipeline taking 10 seconds to scrape
    await sleep(10000); 
    
    // Example: This is the data your pipeline will eventually produce
    const finalLeads = [
      // { company_name: "Example", domain: "example.com", ... }
    ]; 

    // Phase 5: Upsert to Database
    if (finalLeads.length > 0) {
      console.log(`   -> Saving ${finalLeads.length} leads to database...`);
      
      const { data: insertedLeads, error: upsertError } = await supabase
        .from('leads')
        .upsert(finalLeads, { onConflict: 'domain' })
        .select('id');

      if (upsertError) throw upsertError;

      // Map the leads to the specific user who searched for them
      const junctionData = insertedLeads.map((lead) => ({
        user_id: job.user_id,
        lead_id: lead.id,
      }));

      await supabase.from('user_leads').upsert(junctionData, { onConflict: 'user_id,lead_id' });
    }

    return true; 
  } catch (error) {
    throw error; 
  }
}

// ─── THE DATABASE POLLER (STRICT FIFO QUEUE) ──────────────────────
async function startWorker() {
  console.log("👷 Backend Worker Online. Polling for 'pending' jobs...");

  while (true) {
    try {
      // 1. Ask DB for the oldest pending job
      const { data: jobs, error: fetchError } = await supabase
        .from('lead_preferences')
        .select('*')
        .eq('status', 'pending')
        .order('created_at', { ascending: true })
        .limit(1);

      if (fetchError) throw fetchError;

      if (jobs && jobs.length > 0) {
        const job = jobs[0];
        console.log(`\n📥 Grabbed pending job: ${job.id}`);

        // 2. LOCK THE JOB (Update to 'processing')
        const { data: lockedJob, error: lockError } = await supabase
          .from('lead_preferences')
          .update({ status: 'processing', started_at: new Date().toISOString() })
          .eq('id', job.id)
          .eq('status', 'pending') // Double-check it wasn't grabbed by another worker
          .select()
          .single();

        if (lockError || !lockedJob) {
          console.log("   ⚠️ Job lock failed (maybe already processing). Skipping.");
          continue; 
        }

        // 3. EXECUTE THE SCRAPING PIPELINE
        try {
          await executeExtractionPipeline(lockedJob);
          
          // 4. MARK COMPLETED (Frontend detects this and shows leads!)
          await supabase
            .from('lead_preferences')
            .update({ status: 'completed', completed_at: new Date().toISOString() })
            .eq('id', lockedJob.id);
            
          console.log(`✅ Job Completed Successfully.`);
          
        } catch (pipelineError) {
          console.error(`❌ Pipeline Crashed:`, pipelineError.message);
          
          // 5. MARK FAILED (Frontend detects this and shows error)
          await supabase
            .from('lead_preferences')
            .update({ status: 'failed', error_log: pipelineError.message })
            .eq('id', lockedJob.id);
        }

      } else {
        // Queue is empty. Rest for 5 seconds to save database compute.
        await sleep(5000);
      }

    } catch (globalError) {
      console.error("⚠️ Database connection error:", globalError.message);
      await sleep(10000); // Rest longer if database drops connection
    }
  }
}

// Boot up!
startWorker();