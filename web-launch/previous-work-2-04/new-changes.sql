-- 1. Safely rename your existing tables (Keeps all your data!)
ALTER TABLE public.leads RENAME TO prospects;
ALTER TABLE public.user_leads RENAME TO user_prospects;
ALTER TABLE public.lead_preferences RENAME TO user_preferences;

-- 2. Update the Foreign Keys in the Feedback table to match the new names
ALTER TABLE public.feedback RENAME COLUMN lead_id TO prospect_id;

-- 3. Create the brand new Bronze Layer for raw API dumps
CREATE TABLE public.raw_prospects (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  source text NOT NULL, 
  search_query text,
  raw_payload jsonb NOT NULL, 
  domain text,
  place_id text,
  processing_status text DEFAULT 'processed'::text, 
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT raw_prospects_pkey PRIMARY KEY (id)
);

-- 4. Add the linking column to your newly renamed prospects table
ALTER TABLE public.prospects ADD COLUMN raw_prospect_id uuid REFERENCES public.raw_prospects(id);
ALTER TABLE public.prospects ADD COLUMN unlock_count integer DEFAULT 0;
ALTER TABLE public.prospects ADD COLUMN is_sold_out boolean DEFAULT false;