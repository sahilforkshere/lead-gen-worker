-- ═══════════════════════════════════════════════════════════════════════════════
-- Lead-Alert: Required SQL Migrations
-- Run these in Supabase SQL Editor (Dashboard → SQL Editor → New Query)
-- ═══════════════════════════════════════════════════════════════════════════════


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. PERFORMANCE INDEX FOR WORKER POLLING
--    The worker runs this every 5 seconds:
--      SELECT * FROM lead_preferences
--      WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1
--    Without an index, this is a sequential scan on every poll.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_lead_preferences_pending_fifo
  ON public.lead_preferences (created_at ASC)
  WHERE status = 'pending';


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. AUTO-UPDATE `updated_at` TRIGGER
--    The schema has updated_at columns but nothing keeps them current.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.handle_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply to lead_preferences
DROP TRIGGER IF EXISTS set_updated_at ON public.lead_preferences;
CREATE TRIGGER set_updated_at
  BEFORE UPDATE ON public.lead_preferences
  FOR EACH ROW
  EXECUTE FUNCTION public.handle_updated_at();

-- Apply to leads
DROP TRIGGER IF EXISTS set_updated_at ON public.leads;
CREATE TRIGGER set_updated_at
  BEFORE UPDATE ON public.leads
  FOR EACH ROW
  EXECUTE FUNCTION public.handle_updated_at();

-- Apply to profiles
DROP TRIGGER IF EXISTS set_updated_at ON public.profiles;
CREATE TRIGGER set_updated_at
  BEFORE UPDATE ON public.profiles
  FOR EACH ROW
  EXECUTE FUNCTION public.handle_updated_at();


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. ROW LEVEL SECURITY (RLS) POLICIES
--    The worker uses SERVICE_ROLE_KEY (bypasses RLS).
--    The frontend uses the ANON/public key → needs RLS policies or it gets
--    empty results on every query.
-- ─────────────────────────────────────────────────────────────────────────────

-- ── profiles ──────────────────────────────────────────────────────────────────

ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can read own profile"
  ON public.profiles FOR SELECT
  USING (auth.uid() = id);

CREATE POLICY "Users can update own profile"
  ON public.profiles FOR UPDATE
  USING (auth.uid() = id)
  WITH CHECK (auth.uid() = id);

-- ── lead_preferences ──────────────────────────────────────────────────────────

ALTER TABLE public.lead_preferences ENABLE ROW LEVEL SECURITY;

-- Users can read their own job status (for polling / realtime)
CREATE POLICY "Users can read own preferences"
  ON public.lead_preferences FOR SELECT
  USING (auth.uid() = user_id);

-- Users can insert their own search (frontend submit)
CREATE POLICY "Users can insert own preferences"
  ON public.lead_preferences FOR INSERT
  WITH CHECK (auth.uid() = user_id);

-- Users can update their own row (for re-searching / reset)
CREATE POLICY "Users can update own preferences"
  ON public.lead_preferences FOR UPDATE
  USING (auth.uid() = user_id)
  WITH CHECK (auth.uid() = user_id);

-- ── leads ─────────────────────────────────────────────────────────────────────

ALTER TABLE public.leads ENABLE ROW LEVEL SECURITY;

-- Users can only read leads linked to them via user_leads
CREATE POLICY "Users can read their linked leads"
  ON public.leads FOR SELECT
  USING (
    EXISTS (
      SELECT 1 FROM public.user_leads
      WHERE user_leads.lead_id = leads.id
        AND user_leads.user_id = auth.uid()
    )
  );

-- ── user_leads ────────────────────────────────────────────────────────────────

ALTER TABLE public.user_leads ENABLE ROW LEVEL SECURITY;

-- Users can read their own junction rows
CREATE POLICY "Users can read own user_leads"
  ON public.user_leads FOR SELECT
  USING (auth.uid() = user_id);


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. FRONTEND SEARCH FUNCTION (Upsert + Reset)
--    Since user_id is UNIQUE on lead_preferences, re-searching must:
--      a) Upsert the new query (overwrite old search)
--      b) Reset status back to 'pending'
--      c) Clear old timestamps and errors
--      d) Delete old user_leads mappings so the dashboard shows fresh results
--
--    Call from frontend: supabase.rpc('submit_search', { query_text: '...' })
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.submit_search(query_text text)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_user_id uuid := auth.uid();
  v_pref_id uuid;
BEGIN
  -- Guard: must be authenticated
  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'Not authenticated';
  END IF;

  -- Guard: query cannot be empty
  IF trim(query_text) = '' THEN
    RAISE EXCEPTION 'Search query cannot be empty';
  END IF;

  -- Upsert the preference row (insert or overwrite)
  INSERT INTO public.lead_preferences (user_id, search_query, status, created_at, started_at, completed_at, error_message)
  VALUES (v_user_id, trim(query_text), 'pending', now(), NULL, NULL, NULL)
  ON CONFLICT (user_id)
  DO UPDATE SET
    search_query  = EXCLUDED.search_query,
    status        = 'pending',
    created_at    = now(),
    started_at    = NULL,
    completed_at  = NULL,
    error_message = NULL
  RETURNING id INTO v_pref_id;

  -- Clear old lead mappings so the dashboard starts fresh
  DELETE FROM public.user_leads WHERE user_id = v_user_id;

  RETURN v_pref_id;
END;
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. ENABLE SUPABASE REALTIME
--    The doc says "Supabase Realtime detects status changes and pushes to
--    the frontend." This requires the table to be added to the Realtime
--    publication.
--
--    NOTE: If you get "publication supabase_realtime does not exist",
--    create it first with:
--      CREATE PUBLICATION supabase_realtime;
-- ─────────────────────────────────────────────────────────────────────────────

ALTER PUBLICATION supabase_realtime ADD TABLE public.lead_preferences;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. OPTIONAL: RETRY FAILED JOBS
--    The doc mentions retry capability. This function lets you retry all
--    failed jobs (or a specific one) by resetting status to 'pending'.
--
--    Usage from dashboard or admin:
--      SELECT retry_failed_jobs();             -- retry ALL failed
--      SELECT retry_failed_jobs('some-uuid');  -- retry specific job
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.retry_failed_jobs(target_id uuid DEFAULT NULL)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  affected integer;
BEGIN
  IF target_id IS NOT NULL THEN
    UPDATE public.lead_preferences
    SET status = 'pending', started_at = NULL, completed_at = NULL, error_message = NULL, created_at = now()
    WHERE id = target_id AND status = 'failed';
  ELSE
    UPDATE public.lead_preferences
    SET status = 'pending', started_at = NULL, completed_at = NULL, error_message = NULL, created_at = now()
    WHERE status = 'failed';
  END IF;

  GET DIAGNOSTICS affected = ROW_COUNT;
  RETURN affected;
END;
$$;