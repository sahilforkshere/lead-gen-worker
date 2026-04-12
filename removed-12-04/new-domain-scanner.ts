// ═══════════════════════════════════════════════════════════════════════════════
// 🚀 NEW-DOMAIN-SCANNER v5.2 — TIMEOUT-PROOF + BETTER KEYWORDS
// ═══════════════════════════════════════════════════════════════════════════════
// ❌ No WHOIS (credits over)   ❌ No Apollo (saving for later)
// ✅ OpenAI smart keywords      ✅ Serper for ALL contacts
// ✅ Email guessing backup      ✅ Only saves leads WITH contact
// ✅ Multi-niche variety        ✅ Finishes in ~50-70 seconds
// ═══════════════════════════════════════════════════════════════════════════════

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.0";

const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
const supabaseServiceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const WHOISXML_KEY = Deno.env.get("WHOISXML_KEY");
const WEBSITELAUNCHES_KEY = Deno.env.get("WEBSITELAUNCHES_KEY");
const SERPER_API_KEY = Deno.env.get("SERPER_API_KEY");
const OPENAI_API_KEY = Deno.env.get("OPENAI_API_KEY");

// ── LIMITS (total ~50-70s) ───────────────────────────────────────────────────
const MAX_DISCOVERY   = 500;  // More keywords = more domains to find
const MAX_WEBL        = 60;
const MAX_SERPER      = 25;
const MAX_EMAIL_GUESS = 5;     // ⬇️ Was 15 — was taking 117 seconds!
const DOMAIN_AGE_DAYS = 30;
const MAX_AUTHORITY   = 5;
const CRAWL_TIMEOUT   = 2000;

// ── ENDPOINTS ────────────────────────────────────────────────────────────────
const DISCOVERY_URL = "https://domains-subdomains-discovery.whoisxmlapi.com/api/v1";
const EMAIL_VERIFY_URL = "https://emailverification.whoisxmlapi.com/api/v3";
const WEBL_URL = "https://websitelaunches.com/api/v1";
const SERPER_URL = "https://google.serper.dev/search";
const OPENAI_URL = "https://api.openai.com/v1/chat/completions";

// ── VALID TLDs ───────────────────────────────────────────────────────────────
const VALID_TLDS = [
  ".com",".net",".org",".info",".biz",".co.uk",".uk",".us",".ca",
  ".com.au",".au",".nz",".ie",".de",".fr",".it",".es",".nl",".be",
  ".at",".ch",".se",".no",".dk",".fi",".pt",".pl",".cz",".hu",".ro",
  ".in",".co.in",".jp",".kr",".sg",".com.br",".mx",".co",".cl",".ar",
  ".io",".pro",".studio",".agency",".services",".dental",".fitness",
  ".salon",".repair",".plumbing",".cleaning",".photography",".restaurant",
  ".cafe",".legal",".realty",
];

// ── SPAM SIGNALS ─────────────────────────────────────────────────────────────
const SALE_SIGNALS = [
  "domain is for sale","buy this domain","make an offer","domain for sale",
  "sedo.com","afternic.com","dan.com","hugedomains.com","bodis.com",
  "parked domain","parked by","domain parking","is available to register",
  "domain has expired","this domain is available","register your domain",
  "congratulations! your new site","welcome to nginx","apache2 default",
  "under construction","website coming soon","future home of",
  "global domains international","website.ws","first year","each additional year",
];

const JUNK_EMAILS = ["privacy","protect","redacted","withheld","noreply",
  "proxy","masked","example.com","sentry","wixpress","schema.org","noreply"];

// ═══════════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════════
type Lead = {
  domain: string; company: string; email: string | null;
  phone: string | null; source: string; authority: number; age_days: number;
};

function company(domain: string): string {
  return domain
    .replace(/\.(com|net|org|io|co\.uk|co|biz|info|in|us|ca|au|de|fr|nl|it|es|jp|br|kr|fi|se|no|dk|be|at|ch|pt|pl)$/i, "")
    .replace(/[-_.]/g, " ").replace(/\b\w/g, c => c.toUpperCase()).trim();
}

function isJunk(d: string): boolean {
  if (/^[a-z]{20,}\./.test(d) || /^[0-9]{4,}\./.test(d) || /forsale/i.test(d)) return true;
  if (/\d{6,}/.test(d) || /^(buy|sell|get|best|top|cheap|free|premium|domain)/i.test(d)) return true;
  if (!VALID_TLDS.some(t => d.endsWith(t))) return true;
  const name = d.replace(/\.[^.]+(\.[^.]+)?$/, "");
  if (name.length <= 2 || name.length >= 30) return true;
  const alpha = name.replace(/[^a-zA-Z]/g, "");
  if (alpha.length > 6 && (alpha.match(/[aeiou]/gi) || []).length / alpha.length < 0.15) return true;
  return false;
}

function findEmails(text: string): string[] {
  return (text.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g) || [])
    .filter(e => !JUNK_EMAILS.some(j => e.toLowerCase().includes(j)));
}

function findPhones(text: string): string[] {
  return (text.match(/(?:\+?\d{1,3}[-.\s]?)?\(?\d{2,4}\)?[-.\s]?\d{3,4}[-.\s]?\d{3,4}/g) || [])
    .filter(p => { const d = p.replace(/\D/g, ""); return d.length >= 7 && d.length <= 15; });
}

async function batch<T, R>(items: T[], size: number, fn: (x: T) => Promise<R>, delay = 150): Promise<R[]> {
  const out: R[] = [];
  for (let i = 0; i < items.length; i += size) {
    const res = await Promise.allSettled(items.slice(i, i + size).map(fn));
    for (const r of res) if (r.status === "fulfilled") out.push(r.value);
    if (i + size < items.length) await new Promise(r => setTimeout(r, delay));
  }
  return out;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 0: OPENAI — Smart keywords (~2s)
// ═══════════════════════════════════════════════════════════════════════════════

// Hardcoded reliable keywords per niche (OpenAI backup)
const NICHE_KW: Record<string, string[]> = {
  "restaurant":     ["restaurant", "cafe", "bistro", "eatery", "diner"],
  "plumber":        ["plumber", "plumbing", "heating", "drainage"],
  "electrician":    ["electrician", "electrical", "wiring", "sparky"],
  "salon":          ["salon", "beauty", "barber", "nails", "hairstyl"],
  "dental":         ["dental", "dentist", "orthodont", "smileclinic"],
  "gym":            ["gym", "fitness", "crossfit", "workout", "training"],
  "lawyer":         ["lawyer", "attorney", "solicitor", "lawfirm", "legal"],
  "cleaning":       ["cleaning", "cleaner", "maidservice", "janitorial"],
  "photography":    ["photography", "photographer", "photostudio"],
  "construction":   ["construction", "builder", "contractor", "roofing"],
  "landscaping":    ["landscap", "gardener", "lawncare", "yardwork"],
  "auto repair":    ["autorepair", "mechanic", "autoshop", "carbody"],
  "moving company": ["moving", "mover", "removals", "hauling", "relocation"],
  "wedding":        ["wedding", "bridal", "weddingplan"],
  "hvac":           ["hvac", "airconditioning", "heating"],
  "pest control":   ["pestcontrol", "exterminator", "termite"],
  "accountant":     ["accounting", "accountant", "bookkeep", "taxservice"],
  "realEstate":     ["realestate", "realtor", "property", "homesale"],
};

const GENERAL_KW = [
  "restaurant", "plumber", "dentist", "salon", "florist",
  "bakery", "barber", "mechanic", "roofing", "painter",
  "electrician", "landscap", "veterinar", "chiropract", "optician",
  "cleaning", "moving", "yoga", "pharmacy", "pizzeria",
];

async function smartKeywords(niche: string): Promise<string[]> {
  const isMulti = !niche || niche === "any" || niche === "general" || niche === "mixed";

  if (!OPENAI_API_KEY) {
    console.log(`   [⚠️] No OPENAI_API_KEY — using fallbacks`);
    if (isMulti) return [...GENERAL_KW].sort(() => Math.random() - 0.5).slice(0, 10);
    const nicheKw = NICHE_KW[niche];
    if (nicheKw) return nicheKw;
    return [niche.split(" ")[0].toLowerCase()];
  }

  const prompt = isMulti
    ? `Generate 12 domain-name keywords for finding NEW local businesses.

RULES:
- SHORT SINGLE WORD only (4-12 characters): "plumber", "dentist", "salon"
- NOT compound words: "plumbingexperts" = BAD, "plumber" = GOOD
- ONLY physical/local businesses that have Google Maps listings with phone numbers
- BEST niches (easy to find contacts): plumber, dentist, restaurant, mechanic, salon, florist, bakery, veterinarian, chiropractor, optician, roofing, painter, barber, electrician, landscaper
- AVOID: tech companies, software, digital, security, cloud, SaaS, online-only businesses — these DON'T have Google Maps listings

Return ONLY a JSON array.`
    : `Generate 6 SHORT keywords for "${niche}" businesses in domain names.

RULES:
- SHORT SINGLE WORD (4-12 chars): "moving" not "movingcompany"
- Words that LOCAL physical businesses use in their domain names
- GOOD for "moving company": ["moving","mover","removals","hauling","relocation","freight"]
- BAD: ["movingcompany","relocationexperts"] — too long!

Return ONLY a JSON array.`;

  try {
    const res = await fetch(OPENAI_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${OPENAI_API_KEY}` },
      body: JSON.stringify({
        model: "gpt-4o-mini", max_tokens: 150, temperature: 0.7,
        messages: [{ role: "user", content: prompt }],
      }),
    });
    if (!res.ok) throw new Error(`${res.status}`);
    const text = (await res.json() as any).choices?.[0]?.message?.content?.trim() || "";
    let kw = JSON.parse(text.replace(/```json|```/g, "").trim()) as string[];

    // Safety: filter out any compound words that are too long
    kw = kw.filter(k => k.length <= 14).map(k => k.toLowerCase().replace(/\s+/g, ""));

    console.log(`   [🧠] OpenAI keywords: ${kw.join(", ")}`);

    // For single niche: also mix in 3-4 general keywords for variety
    if (!isMulti && kw.length < 10) {
      const extras = [...GENERAL_KW]
        .filter(g => !kw.includes(g))
        .sort(() => Math.random() - 0.5)
        .slice(0, 4);
      kw.push(...extras);
      console.log(`   [🎲] Mixed in general keywords: ${extras.join(", ")}`);
    }

    return kw.slice(0, 12);
  } catch (err) {
    console.error(`   [❌] OpenAI: ${(err as Error).message}`);
    if (isMulti) return [...GENERAL_KW].sort(() => Math.random() - 0.5).slice(0, 10);
    return NICHE_KW[niche] || [niche.split(" ")[0].toLowerCase()];
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 1: DISCOVER NEW DOMAINS (~5-8s)
// ═══════════════════════════════════════════════════════════════════════════════
async function discover(keywords: string[], since: string): Promise<{ domain: string; kw: string }[]> {
  if (!WHOISXML_KEY) throw new Error("Missing WHOISXML_KEY");
  const all = new Map<string, string>();

  for (const kw of keywords) {
    try {
      const res = await fetch(DISCOVERY_URL, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ apiKey: WHOISXML_KEY, sinceDate: since, domains: { include: [`*${kw}*`] } }),
      });
      const data = await res.json() as any;
      if (data.code === 403) { console.log(`      ⚠️ ${kw}: 403`); continue; }
      let added = 0;
      for (const d of (data.domainsList || []) as string[]) {
        const c = d.toLowerCase().trim();
        if (c && !all.has(c)) { all.set(c, kw); added++; }
        if (all.size >= MAX_DISCOVERY) break;
      }
      console.log(`   [🔍] *${kw}* → +${added} (total: ${all.size})`);
      if (all.size >= MAX_DISCOVERY) break;
      await new Promise(r => setTimeout(r, 200));
    } catch {}
  }
  console.log(`   [✅] Discovery: ${all.size} domains`);
  return Array.from(all.entries()).map(([domain, kw]) => ({ domain, kw }));
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 2: WEBSITELAUNCHES — Is it unbuilt? (~4-6s)
// ═══════════════════════════════════════════════════════════════════════════════
async function filterUnbuilt(domains: { domain: string; kw: string }[]): Promise<Lead[]> {
  if (!WEBSITELAUNCHES_KEY) {
    return domains.slice(0, MAX_WEBL).map(d => ({
      domain: d.domain, company: company(d.domain),
      email: null, phone: null, source: "", authority: 0, age_days: 0,
    }));
  }
  const slice = domains.slice(0, MAX_WEBL);
  console.log(`   [🔍] WebsiteLaunches: ${slice.length} domains...`);

  const check = async (d: { domain: string; kw: string }): Promise<Lead | null> => {
    try {
      const res = await fetch(`${WEBL_URL}/domain/${d.domain}`, {
        headers: { "Content-Type": "application/json", "X-API-Key": WEBSITELAUNCHES_KEY },
      });
      if (!res.ok) return null;
      const r = (await res.json() as any)?.data;
      if (!r) return null;
      const auth = r.site_authority ?? r.domain_authority ?? 0;
      if (r.launch_detected === true || auth > MAX_AUTHORITY) return null;
      if ((r.category || "").toLowerCase().includes("parked")) return null;
      return {
        domain: d.domain, company: company(d.domain),
        email: null, phone: null, source: "",
        authority: auth, age_days: Math.floor((r.domain_age ?? 0) * 365),
      };
    } catch { return null; }
  };

  const results = await batch(slice, 15, check, 200);
  const unbuilt = results.filter((r): r is Lead => r !== null);
  console.log(`   [✅] Unbuilt: ${unbuilt.length}`);
  return unbuilt;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 2.5: LIVE CRAWL — Catch for-sale + built sites (~6-8s)
// ═══════════════════════════════════════════════════════════════════════════════
async function crawlFilter(leads: Lead[]): Promise<Lead[]> {
  console.log(`   [🌐] Crawl: ${leads.length} domains...`);
  let sale = 0, built = 0, empty = 0, tout = 0;

  const check = async (lead: Lead): Promise<Lead | null> => {
    try {
      const ctrl = new AbortController();
      const t = setTimeout(() => ctrl.abort(), CRAWL_TIMEOUT);
      let res;
      try {
        res = await fetch(`https://${lead.domain}`, { redirect: "follow", signal: ctrl.signal, headers: { "User-Agent": "Mozilla/5.0" } });
      } catch {
        if (ctrl.signal.aborted) throw "timeout";
        try { res = await fetch(`http://${lead.domain}`, { redirect: "follow", signal: ctrl.signal, headers: { "User-Agent": "Mozilla/5.0" } }); }
        catch { clearTimeout(t); throw "unreachable"; }
      }
      clearTimeout(t);

      if (!res.ok && res.status >= 500) { empty++; return lead; }
      const reader = res.body?.getReader();
      if (!reader) { empty++; return lead; }
      let html = "", bytes = 0;
      const dec = new TextDecoder();
      while (bytes < 10000) {
        const { done, value } = await reader.read(); if (done) break;
        html += dec.decode(value, { stream: true }); bytes += value.length;
      }
      reader.cancel();
      const h = html.toLowerCase();

      if (SALE_SIGNALS.some(s => h.includes(s))) { sale++; return null; }

      let score = 0;
      if (h.includes("<nav")) score += 2;
      if ((h.match(/href=["']\//g) || []).length >= 3) score += 2;
      if ((h.match(/<img/g) || []).length >= 3) score += 2;
      if (h.includes('stylesheet')) score += 1;
      if (h.includes("<footer")) score += 1;
      if (bytes > 6000) score += 2;
      if (h.includes("<form")) score += 1;
      if (score >= 5) { built++; return null; }

      empty++;
      return lead;
    } catch { tout++; return lead; }
  };

  const results = await batch(leads, 15, check, 100);
  const kept = results.filter((r): r is Lead => r !== null);
  console.log(`   [✅] Crawl: ${kept.length} kept | ${sale} for-sale | ${built} built | ${tout} timeout`);
  return kept;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 3: SERPER — Find contacts via Google (~8-12s)
// ═══════════════════════════════════════════════════════════════════════════════
async function serperEnrich(leads: Lead[]): Promise<void> {
  if (!SERPER_API_KEY) { console.log(`   [⚠️] No SERPER_API_KEY!`); return; }

  const toSearch = leads.filter(l => !l.email && !l.phone).slice(0, MAX_SERPER);
  console.log(`   [🔎] Serper: ${toSearch.length} leads...`);
  let found = 0;

  const search = async (lead: Lead): Promise<void> => {
    try {
      const res = await fetch(SERPER_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-API-KEY": SERPER_API_KEY },
        body: JSON.stringify({ q: `"${lead.company}" contact email phone`, num: 5 }),
      });
      if (!res.ok) return;
      const data = await res.json() as any;

      // Knowledge Graph (most reliable)
      const kg = data.knowledgeGraph;
      if (kg) {
        if (!lead.phone) lead.phone = kg.phone || kg.telephone || kg.attributes?.Phone || kg.attributes?.phone || null;
        if (!lead.email) lead.email = kg.email || kg.attributes?.Email || kg.attributes?.email || null;
      }

      // Places (Google Maps)
      const place = (data.places || [])[0];
      if (place && !lead.phone) lead.phone = place.phone || place.phoneNumber || null;

      // Organic results
      for (const r of (data.organic || []).slice(0, 5)) {
        const txt = `${r.title || ""} ${r.snippet || ""}`;
        if (!lead.email) { const e = findEmails(txt); if (e.length) lead.email = e[0]; }
        if (!lead.phone) { const p = findPhones(txt); if (p.length) lead.phone = p[0]; }
        if (lead.email && lead.phone) break;
      }

      if (lead.email || lead.phone) {
        lead.source = "serper";
        found++;
        console.log(`      🔎 ${lead.company} | ${lead.email || "no email"} | ${lead.phone || "no phone"}`);
      }
    } catch {}
  };

  await batch(toSearch, 5, search, 150);
  console.log(`   [✅] Serper: ${found}/${toSearch.length} found contacts`);
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 3.5: SERPER DEEP — Search "@domain.com" for email (~3-5s)
// ═══════════════════════════════════════════════════════════════════════════════
async function serperDeep(leads: Lead[]): Promise<void> {
  if (!SERPER_API_KEY) return;

  const noEmail = leads.filter(l => !l.email).slice(0, 10);
  if (noEmail.length === 0) return;
  console.log(`   [🔎] Deep search: ${noEmail.length} leads...`);
  let found = 0;

  const search = async (lead: Lead): Promise<void> => {
    try {
      const res = await fetch(SERPER_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-API-KEY": SERPER_API_KEY },
        body: JSON.stringify({ q: `"@${lead.domain}" OR "${lead.company}" email contact`, num: 5 }),
      });
      if (!res.ok) return;
      const data = await res.json() as any;
      const allText = (data.organic || []).map((r: any) => `${r.title} ${r.snippet}`).join(" ");
      const emails = findEmails(allText);
      const match = emails.find(e => e.endsWith(`@${lead.domain}`)) || emails[0];
      if (match) {
        lead.email = match;
        if (!lead.source) lead.source = "serper";
        found++;
        console.log(`      🔎 Deep: ${lead.domain} → ${match}`);
      }
    } catch {}
  };

  await batch(noEmail, 5, search, 150);
  console.log(`   [✅] Deep: ${found} emails found`);
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 4: EMAIL GUESSING — Parallel, max ~8 seconds
// ═══════════════════════════════════════════════════════════════════════════════
async function guessEmails(leads: Lead[]): Promise<void> {
  if (!WHOISXML_KEY) return;

  const noEmail = leads.filter(l => !l.email).slice(0, MAX_EMAIL_GUESS);
  if (noEmail.length === 0) return;
  console.log(`   [📧] Guessing emails for ${noEmail.length} leads...`);
  let found = 0;

  // Try info@ and contact@ for each domain IN PARALLEL
  const tryDomain = async (lead: Lead): Promise<void> => {
    for (const prefix of ["info", "contact"]) {
      const test = `${prefix}@${lead.domain}`;
      try {
        const res = await fetch(`${EMAIL_VERIFY_URL}?apiKey=${WHOISXML_KEY}&emailAddress=${encodeURIComponent(test)}`);
        if (!res.ok) continue;
        const data = await res.json() as any;
        if (data?.dnsCheck !== "true") return; // No mail server → skip domain entirely
        if (data?.formatCheck === "true" && data?.smtpCheck === "true" && data?.disposableCheck !== "true") {
          lead.email = test;
          if (!lead.source) lead.source = "guess";
          found++;
          console.log(`      📧 Verified: ${test}`);
          return; // Found one, stop
        }
      } catch { continue; }
    }
  };

  // Run all domains in parallel (batch of 5 = ~3 seconds total)
  await batch(noEmail, 5, tryDomain, 100);
  console.log(`   [✅] Guess: ${found} verified emails`);
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════════
serve(async (req) => {
  const supabase = createClient(supabaseUrl, supabaseServiceKey);
  const startTime = Date.now();

  try {
    const { user_id, niche = "any", location = "London", target_count = 30, sources = ["new_domain"] } = await req.json();
    if (!user_id) throw new Error("Missing user_id");
    if (!sources.includes("new_domain")) {
      return new Response(JSON.stringify({ success: true, saved_count: 0 }), { status: 200 });
    }

    const target = Math.ceil(target_count / sources.length);
    const since = new Date(Date.now() - DOMAIN_AGE_DAYS * 86_400_000).toISOString().split("T")[0];

    console.log(`\n${"═".repeat(60)}\n🚀 v5.2 | ${user_id} | niche: ${niche} | target: ${target}\n${"═".repeat(60)}`);

    // ── STEP 0: Smart keywords via OpenAI ──
    const keywords = await smartKeywords(niche);

    // ── STEP 1: Discover new domains ──
    const raw = await discover(keywords, since);
    if (raw.length === 0) return new Response(JSON.stringify({ success: true, saved_count: 0, message: "No domains" }), { status: 200 });

    // ── STEP 1.5: Junk filter ──
    const clean = raw.filter(d => !isJunk(d.domain));
    console.log(`   [🧹] Junk filter: ${raw.length} → ${clean.length}`);
    if (clean.length === 0) return new Response(JSON.stringify({ success: true, saved_count: 0, message: "All junk" }), { status: 200 });

    // ── STEP 2: WebsiteLaunches ──
    const unbuilt = await filterUnbuilt(clean);
    if (unbuilt.length === 0) return new Response(JSON.stringify({ success: true, saved_count: 0, message: "All launched" }), { status: 200 });

    // ── STEP 2.5: Live crawl ──
    const genuine = await crawlFilter(unbuilt);
    if (genuine.length === 0) return new Response(JSON.stringify({ success: true, saved_count: 0, message: "All for-sale/built" }), { status: 200 });

    // ── STEP 3: Serper enrichment ──
    await serperEnrich(genuine);

    // ── STEP 3.5: Serper deep email search (skip if running low on time) ──
    const elapsed1 = (Date.now() - startTime) / 1000;
    if (elapsed1 < 100) {
      await serperDeep(genuine);
    } else {
      console.log(`   [⏰] Skipping deep search — ${elapsed1.toFixed(0)}s elapsed`);
    }

    // ── STEP 4: Email guessing (skip if running low on time) ──
    const elapsed2 = (Date.now() - startTime) / 1000;
    if (elapsed2 < 120) {
      await guessEmails(genuine);
    } else {
      console.log(`   [⏰] Skipping email guess — ${elapsed2.toFixed(0)}s elapsed`);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 🆕 ONLY SAVE LEADS WITH CONTACT — no more tier_4 junk
    // ═══════════════════════════════════════════════════════════════════════
    const qualified = genuine.filter(l => l.email || l.phone).slice(0, target);
    console.log(`\n   [🎯] Qualified leads: ${qualified.length} (out of ${genuine.length} genuine domains)`);

    if (qualified.length === 0) {
      console.log(`   [😞] No contacts found. Try different niches or check API keys.`);
      return new Response(JSON.stringify({ success: true, saved_count: 0, message: "No contacts found" }), { status: 200 });
    }

    // ── Build rows ──
    const rawRows: any[] = [];
    const cleanRows: any[] = [];

    for (const lead of qualified) {
      let tier = "tier_4_domain_only";
      if (lead.email && lead.phone) tier = "tier_1_verified";
      else if (lead.email) tier = "tier_1_verified";
      else if (lead.phone) tier = "tier_2_phone_only";

      let pitch = "Search LinkedIn for founder";
      if (lead.email) pitch = `Email ${lead.company} directly (${lead.source})`;
      else if (lead.phone) pitch = `Call ${lead.company} directly (${lead.source})`;

      console.log(`      ✅ ${lead.domain} | ${lead.email || "—"} | ${lead.phone || "—"} | ${tier}`);

      const rawId = crypto.randomUUID();
      rawRows.push({
        id: rawId, source: "whois_discovery", search_query: `${niche} new domains`,
        domain: lead.domain, raw_payload: { lead },
      });
      cleanRows.push({
        raw_prospect_id: rawId, domain: lead.domain, company_name: lead.company,
        phone: lead.phone, email: lead.email, address: location,
        listing_url: `https://${lead.domain}`, source: "new_domain",
        search_query: `${niche} new domains`, business_type: "Brand New Startup",
        lead_tier: tier, pitch_angle: pitch,
        has_website: false, domain_launched: false,
        domain_age_days: lead.age_days, domain_authority: lead.authority,
        domain_registered_date: since,
      });
    }

    // ── Save to DB ──
    if (rawRows.length > 0) {
      const { error } = await supabase.from("raw_prospects").insert(rawRows);
      if (error) console.error(`   [❌] Raw: ${error.message}`);
    }

    const { data: inserted, error: err } = await supabase
      .from("prospects").upsert(cleanRows, { onConflict: "domain" }).select("id");
    if (err) { console.error(`   [❌] Prospects: ${err.message}`); throw new Error(err.message); }

    if (inserted?.length) {
      await supabase.from("user_prospects").upsert(
        inserted.map(p => ({ user_id, prospect_id: p.id })),
        { onConflict: "user_id,prospect_id" }
      );
      const { data: profile } = await supabase.from("profiles")
        .select("drip_leads_this_month").eq("id", user_id).single();
      if (profile) {
        await supabase.from("profiles")
          .update({ drip_leads_this_month: (profile.drip_leads_this_month || 0) + inserted.length })
          .eq("id", user_id);
      }
    }

    // ── Stats ──
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const withEmail = qualified.filter(l => l.email).length;
    const withPhone = qualified.filter(l => l.phone).length;
    const stats = {
      discovered: raw.length,
      after_filter: clean.length,
      unbuilt: unbuilt.length,
      genuine: genuine.length,
      qualified: qualified.length,
      saved: inserted?.length || 0,
      with_email: withEmail,
      with_phone: withPhone,
      elapsed_seconds: elapsed,
    };

    console.log(`\n${"═".repeat(60)}`);
    console.log(`🏁 DONE in ${elapsed}s`);
    console.log(`   ${stats.discovered} found → ${stats.after_filter} clean → ${stats.unbuilt} unbuilt → ${stats.genuine} genuine → ${stats.saved} SAVED`);
    console.log(`   📧 ${withEmail} emails | 📞 ${withPhone} phones | ALL leads have contact info`);
    console.log(`${"═".repeat(60)}\n`);

    return new Response(JSON.stringify({ success: true, saved_count: stats.saved, stats }), { status: 200 });

  } catch (error) {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.error(`\n🚨 FATAL (${elapsed}s): ${(error as Error).message}`);
    return new Response(JSON.stringify({ error: (error as Error).message }), { status: 500 });
  }
});