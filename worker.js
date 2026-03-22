import { createClient } from "@supabase/supabase-js";
import * as dotenv from "dotenv";

dotenv.config();

// ─── ENV ──────────────────────────────────────────────────────────────────────
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const OPENAI_API_KEY   = process.env.OPENAI_API_KEY;
const PARALLEL_API_KEY = process.env.PARALLEL_API_KEY;
const TAVILY_API_KEY   = process.env.TAVILY_API_KEY;
const JINA_API_KEY     = process.env.JINA_API_KEY;
const SERPER_API_KEY   = process.env.SERPER_API_KEY;

// ─── CONSTANTS (TUNED FOR 100 LEADS) ─────────────────────────────────────────
const FINAL_OUTPUT_SIZE  = 100;
const EXTRACT_BATCH_SIZE = 5;
const MAX_DIR_PAGES      = 5;
const SUB_QUERY_COUNT    = 14;

// ─── LEAD RANK TIERS ─────────────────────────────────────────────────────────
const TIER_WEBSITE      = "tier_1_website";
const TIER_SOCIAL_MEDIA = "tier_2_social_media";
const TIER_NONE         = "tier_3_none";

const TIER_SCORE_BONUS = {
  [TIER_WEBSITE]:      50,
  [TIER_SOCIAL_MEDIA]: 25,
  [TIER_NONE]:          0,
};

// ─── SOCIAL MEDIA DOMAINS ─────────────────────────────────────────────────────
const SOCIAL_MEDIA_DOMAINS = [
  "facebook.com", "fb.com", "fb.me",
  "instagram.com",
  "twitter.com", "x.com",
  "linkedin.com",
  "youtube.com", "youtu.be",
  "pinterest.com",
  "tiktok.com",
  "threads.net",
  "snapchat.com",
  "wa.me", "whatsapp.com",
  "t.me", "telegram.me",
];

// ─── DOMAIN LISTS ─────────────────────────────────────────────────────────────
const DIRECTORY_DOMAINS = [
  "justdial.com", "sulekha.com", "zomato.com", "swiggy.com",
  "tripadvisor.", "yelp.", "indiamart.com", "tradeindia.com",
  "magicpin.in", "dineout.co.in", "eazydiner.com", "burrp.com",
  "happytrips.com", "timescity.com", "so.city", "nearbuy.com",
  "yellowpages.", "lbb.in", "whatshot.in",
];

const HARD_BLOCKED = [
  "facebook.com", "instagram.com", "twitter.com", "x.com",
  "linkedin.com", "youtube.com", "youtu.be", "tiktok.com",
  "reddit.com", "quora.com", "pinterest.com", "tumblr.com",
  "wikipedia.org", "wikimedia.org",
  "amazon.com", "amazon.in", "flipkart.com",
  "apps.apple.com", "play.google.com",
];

// ─── HELPERS ──────────────────────────────────────────────────────────────────
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function safeHostname(raw) {
  try { return new URL(raw).hostname.replace("www.", ""); }
  catch { return null; }
}

function safeOrigin(raw) {
  try { return new URL(raw).origin; }
  catch { return null; }
}

function isDirectory(domain) {
  return DIRECTORY_DOMAINS.some((d) => domain.includes(d));
}

function isBlocked(domain) {
  return HARD_BLOCKED.some((b) => domain.includes(b));
}

function normaliseName(name) {
  return name.toLowerCase().replace(/[^a-z0-9\s]/g, "").replace(/\s+/g, " ").trim();
}

function resolveUrl(href, pageUrl) {
  if (!href) return "";
  if (href.startsWith("http://") || href.startsWith("https://")) return href;
  if (href.startsWith("//")) return "https:" + href;
  const origin = safeOrigin(pageUrl);
  if (!origin) return href;
  if (href.startsWith("/")) return origin + href;
  try { return new URL(href, pageUrl).href; }
  catch { return origin + "/" + href; }
}

function isValidHttpUrl(url) {
  if (!url) return false;
  try {
    const u = new URL(url);
    return u.protocol === "https:" || u.protocol === "http:";
  } catch { return false; }
}

function isSocialMediaUrl(url) {
  if (!url) return false;
  const host = safeHostname(url);
  if (!host) return false;
  return SOCIAL_MEDIA_DOMAINS.some((d) => host.includes(d));
}

function isRealBusinessWebsite(url) {
  if (!url) return false;
  if (!isValidHttpUrl(url)) return false;
  const host = safeHostname(url);
  if (!host) return false;
  if (isDirectory(host)) return false;
  if (isSocialMediaUrl(url)) return false;
  if (isBlocked(host)) return false;
  return true;
}

function classifyLeadTier(website, socialMediaLinks) {
  if (isRealBusinessWebsite(website)) {
    return { tier: TIER_WEBSITE, tierLabel: "Has Website" };
  }
  const validSocials = socialMediaLinks.filter((url) => url && isSocialMediaUrl(url));
  if (validSocials.length > 0) {
    return { tier: TIER_SOCIAL_MEDIA, tierLabel: "Has Social Media" };
  }
  return { tier: TIER_NONE, tierLabel: "No Online Presence" };
}

function buildBestLink(biz, listing_url, website, source_url) {
  return listing_url
    || website
    || biz.instagram_url?.trim()
    || biz.facebook_url?.trim()
    || biz.twitter_url?.trim()
    || biz.youtube_url?.trim()
    || biz.other_social_url?.trim()
    || source_url;
}

// ─── GPT JSON HELPER ──────────────────────────────────────────────────────────
async function gptJson(messages, label) {
  try {
    const res = await fetch("https://api.openai.com/v1/chat/completions", {
      method:  "POST",
      headers: {
        "Content-Type":  "application/json",
        "Authorization": `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model:           "gpt-4o-mini",
        response_format: { type: "json_object" },
        messages,
      }),
    });
    const data = await res.json();
    return JSON.parse(data.choices[0].message.content);
  } catch (e) {
    console.error(`gptJson [${label}] failed:`, e.message);
    return null;
  }
}

// ─── JINA AI PAGE READER ──────────────────────────────────────────────────────
async function fetchPageWithJina(url) {
  try {
    const jinaUrl = `https://r.jina.ai/${url}`;
    const res = await fetch(jinaUrl, {
      method: "GET",
      headers: {
        "Authorization":   `Bearer ${JINA_API_KEY}`,
        "Accept":          "text/plain",
        "X-Return-Format": "markdown",
        "X-Retain-Images": "none",
      },
    });
    if (!res.ok) {
      console.warn(`    Jina fetch failed for ${url}: ${res.status}`);
      return "";
    }
    return (await res.text()) ?? "";
  } catch (e) {
    console.warn(`    Jina fetch error for ${url}:`, e.message);
    return "";
  }
}

// ─── SERPER URL SNIPER ────────────────────────────────────────────────────────
async function resolveExactUrlWithSerper(businessName, directoryDomain, location) {
  if (!businessName || !directoryDomain) return "";
  const siteScope = directoryDomain.replace(/\.$/, "");
  const query     = `"${businessName}" ${location} site:${siteScope}`;

  try {
    const res = await fetch("https://google.serper.dev/search", {
      method:  "POST",
      headers: {
        "Content-Type": "application/json",
        "X-API-KEY":    SERPER_API_KEY,
      },
      body: JSON.stringify({ q: query, num: 3 }),
    });
    if (!res.ok) return "";
    const data = await res.json();
    const firstResult = data.organic?.[0]?.link ?? "";
    if (firstResult && firstResult.includes(siteScope)) return firstResult;
    return "";
  } catch { return ""; }
}

// ─── SERPER WEBSITE + SOCIAL ENRICHMENT ───────────────────────────────────────
async function resolveWebsiteAndSocials(businessName, location) {
  if (!businessName) return { website: "", socialLinks: [] };
  const query = `${businessName} ${location} official website`;

  try {
    const res = await fetch("https://google.serper.dev/search", {
      method:  "POST",
      headers: {
        "Content-Type": "application/json",
        "X-API-KEY":    SERPER_API_KEY,
      },
      body: JSON.stringify({ q: query, num: 8 }),
    });
    if (!res.ok) return { website: "", socialLinks: [] };

    const data = await res.json();
    const organicResults = data.organic ?? [];

    let website = "";
    const socialLinks = [];

    for (const r of organicResults) {
      const url = r.link ?? "";
      if (!url) continue;
      const host = safeHostname(url);
      if (!host) continue;

      if (isSocialMediaUrl(url)) {
        socialLinks.push(url);
        continue;
      }
      if (!website && isRealBusinessWebsite(url)) {
        website = url;
      }
    }

    const knowledgeGraph = data.knowledgeGraph;
    if (knowledgeGraph) {
      if (knowledgeGraph.website && !website) {
        if (isRealBusinessWebsite(knowledgeGraph.website)) {
          website = knowledgeGraph.website;
        }
      }
      const profiles = knowledgeGraph.profiles ?? [];
      for (const p of profiles) {
        if (p.link && isSocialMediaUrl(p.link)) {
          socialLinks.push(p.link);
        }
      }
    }

    return { website, socialLinks: [...new Set(socialLinks)] };
  } catch {
    return { website: "", socialLinks: [] };
  }
}

// ─── GPT EXTRACTION PROMPT BUILDER ────────────────────────────────────────────
function buildExtractionPrompt(search_query, target) {
  return `You are a lead extraction assistant.

THE USER'S SEARCH: "${search_query}"
SOURCE PAGE URL: "${target.url}"

${target.isDirectory ? `THIS IS A DIRECTORY / LISTING PAGE (e.g. Justdial, Zomato, Sulekha, Tripadvisor, Yelp).
The content below is Markdown rendered by Jina AI. Links appear as [Text](https://...).
- Return EVERY business listed on this page as a SEPARATE object.
- If 15 businesses are listed, return 15 objects. Do NOT merge or skip any.
- Include businesses even if they only have a name and phone.

CRITICAL — listing_url extraction:
- For EACH business, find its INDIVIDUAL detail/profile page URL.
- In Jina Markdown, links look like: [Business Name](https://www.justdial.com/Delhi/Business-...)
- Copy the URL from parentheses EXACTLY as written.
- ONLY output listing_urls you can literally see in the content.
- If not found, leave listing_url as "" — NEVER invent URLs.
` : `THIS IS A DIRECT BUSINESS WEBSITE.
- Return exactly 1 object for this business.
- Extract every contact detail visible on the page.
- listing_url: "" (not applicable for direct sites).
`}
Rules:
1. company_name: the business name. REQUIRED.
2. phone: copy exactly as written. "" if not found.
3. email: extract if present. "" if not.
4. address: include area/locality/city. "" if not found.
5. website: the business's OWN official website URL (not directory, not social media). "" if not found.
6. listing_url: ONLY real URLs you see in the content. "" if not found.
7. instagram_url: the business's Instagram profile URL if visible. "" if not.
8. facebook_url: the business's Facebook page URL if visible. "" if not.
9. twitter_url: the business's Twitter/X profile URL if visible. "" if not.
10. youtube_url: the business's YouTube channel URL if visible. "" if not.
11. other_social_url: any other social media URL (LinkedIn, TikTok, Pinterest, etc.). "" if not.
12. description: one sentence about the business. "" if unknown.
13. NEVER invent data. Leave missing fields as "".

Return ONLY valid JSON:
{ "businesses": [
  { "company_name": "", "address": "", "phone": "", "email": "", "website": "", "listing_url": "", "instagram_url": "", "facebook_url": "", "twitter_url": "", "youtube_url": "", "other_social_url": "", "description": "" }
] }`;
}

// ─── PROCESS ONE EXTRACTED BUSINESS INTO A LEAD ENTRY ─────────────────────────
function processBusinessToLead({ biz, target, preference_id, search_query, seenNames }) {
  const name = biz.company_name?.trim();
  if (!name) return null;

  const norm = normaliseName(name);
  if (seenNames.has(norm)) return null;
  seenNames.add(norm);

  const website = biz.website?.trim()
    ? biz.website.trim()
    : (target.isDirectory ? "" : target.url);

  let listing_url = "";
  if (target.isDirectory && biz.listing_url?.trim()) {
    const resolved = resolveUrl(biz.listing_url.trim(), target.url);
    listing_url = isValidHttpUrl(resolved) ? resolved : "";
  }

  const socialMediaLinks = [
    biz.instagram_url?.trim()    ?? "",
    biz.facebook_url?.trim()     ?? "",
    biz.twitter_url?.trim()      ?? "",
    biz.youtube_url?.trim()      ?? "",
    biz.other_social_url?.trim() ?? "",
  ].filter((url) => url && isSocialMediaUrl(url));

  const source_url = target.url;
  const best_link  = buildBestLink(biz, listing_url, website, source_url);

  const { tier, tierLabel } = classifyLeadTier(website, socialMediaLinks);
  const tierBonus = TIER_SCORE_BONUS[tier] ?? 0;

  const slug        = norm.replace(/\s+/g, "-").substring(0, 60);
  const websiteHost = website ? safeHostname(website) : null;
  const listingHost = listing_url ? safeHostname(listing_url) : null;

  const domainKey = websiteHost && isRealBusinessWebsite(website)
    ? websiteHost
    : listingHost
      ? `${listingHost}#${slug}`
      : target.isDirectory
        ? `${target.domain}#${slug}`
        : target.domain;

  const lead = {
    preference_id,
    search_query,
    domain: domainKey,
    lead_data: {
      company_name:       name,
      address:            biz.address?.trim()          ?? "",
      phone:              biz.phone?.trim()            ?? "",
      email:              biz.email?.trim()            ?? "",
      website,
      listing_url,
      source_url,
      best_link,
      instagram_url:      biz.instagram_url?.trim()    ?? "",
      facebook_url:       biz.facebook_url?.trim()     ?? "",
      twitter_url:        biz.twitter_url?.trim()      ?? "",
      youtube_url:        biz.youtube_url?.trim()      ?? "",
      other_social_url:   biz.other_social_url?.trim() ?? "",
      social_media_links: socialMediaLinks,
      tier,
      tier_label:         tierLabel,
      description:        biz.description?.trim()      ?? "",
    },
    status: "verified",
  };

  const baseScore =
    (name                    ? 10 : 0) +
    (biz.phone?.trim()       ? 30 : 0) +
    (biz.email?.trim()       ? 30 : 0) +
    (biz.address?.trim()     ? 10 : 0) +
    (listing_url             ?  8 : 0) +
    (biz.description?.trim() ?  5 : 0);

  const locationHint = search_query.replace(/without websites?/gi, "").trim();

  return {
    lead,
    score: baseScore + tierBonus,
    tier,
    tierLabel,
    needsSerper: (target.isDirectory && !listing_url)
      ? { name, domain: target.domain, location: locationHint }
      : undefined,
    needsEnrichment: (tier === TIER_NONE),
    businessName: name,
  };
}

// ─── EXTRACT LEADS FROM A BATCH OF TARGETS ────────────────────────────────────
async function extractFromTargets({ targets, search_query, preference_id, seenNames, allLeads }) {
  for (let i = 0; i < targets.length; i += EXTRACT_BATCH_SIZE) {
    const batch    = targets.slice(i, i + EXTRACT_BATCH_SIZE);
    const batchNum = Math.floor(i / EXTRACT_BATCH_SIZE) + 1;
    const total    = Math.ceil(targets.length / EXTRACT_BATCH_SIZE);
    console.log(`  Batch ${batchNum}/${total}: ${batch.length} targets…`);

    const batchResults = await Promise.all(
      batch.map(async (target) => {
        try {
          let content = "";
          const jinaMarkdown = await fetchPageWithJina(target.url);

          if (jinaMarkdown.length >= 200) {
            content = jinaMarkdown;
            console.log(`    📄 Jina OK: ${target.domain} (${jinaMarkdown.length} chars)`);
          } else {
            content = target.snippet;
            console.warn(`    ⚠️  Jina short for ${target.domain}, using snippet.`);
          }

          if (content.length < 80) {
            console.warn(`    ⚠️  ${target.domain}: too short, skipping.`);
            return [];
          }

          const extracted = await gptJson(
            [
              { role: "system", content: buildExtractionPrompt(search_query, target) },
              { role: "user",   content: content.substring(0, 12000) },
            ],
            `extract-${target.domain}`,
          );

          const businesses = extracted?.businesses ?? [];
          if (businesses.length === 0) return [];

          console.log(`    ✅ ${target.domain}${target.isDirectory ? " [DIR]" : ""}: ${businesses.length} business(es)`);

          const results = [];
          for (const biz of businesses) {
            const entry = processBusinessToLead({
              biz, target, preference_id, search_query, seenNames,
            });
            if (entry) results.push(entry);
          }
          return results;
        } catch (err) {
          console.error(`    ⚠️  ${target.domain}:`, err.message);
          return [];
        }
      }),
    );

    for (const item of batchResults.flat()) {
      allLeads.push(item);
    }

    console.log(`  Running total: ${allLeads.length} unique leads.`);

    if (allLeads.length >= FINAL_OUTPUT_SIZE * 3) {
      console.log(`  🎯 Enough leads (${allLeads.length}) — stopping extraction early.`);
      break;
    }
  }
}

// ─── DISCOVERY: SEARCH APIs ───────────────────────────────────────────────────
async function runDiscovery(queries) {
  const rawResults = [];

  const [parallelPages, tavilyPages] = await Promise.all([
    Promise.all(
      queries.map((q) =>
        fetch("https://api.parallel.ai/v1beta/search", {
          method:  "POST",
          headers: { "Content-Type": "application/json", "x-api-key": PARALLEL_API_KEY },
          body: JSON.stringify({
            objective: q, search_queries: [q],
            mode: "fast", max_results: 40,
            excerpts: { max_chars_per_result: 4000 },
          }),
        }).then((r) => r.json()).catch(() => ({ results: [] }))
      )
    ),
    Promise.all(
      queries.map((q) =>
        fetch("https://api.tavily.com/search", {
          method:  "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            api_key: TAVILY_API_KEY, query: q,
            search_depth: "advanced", max_results: 30,
          }),
        }).then((r) => r.json()).catch(() => ({ results: [] }))
      )
    ),
  ]);

  for (const page of parallelPages) {
    for (const item of (page.results ?? [])) {
      const url = item.url ?? item.content_url;
      if (url) rawResults.push({ url, snippet: item.excerpts?.join(" ") ?? "" });
    }
  }
  for (const page of tavilyPages) {
    for (const item of (page.results ?? [])) {
      if (item.url) rawResults.push({ url: item.url, snippet: item.content ?? "" });
    }
  }

  return rawResults;
}

// ─── FILTER & DEDUP RAW URLs INTO TARGETS ─────────────────────────────────────
function filterAndDedup(rawResults, seenUrls, seenDirectDomains, dirPageCount) {
  const dirTargets    = [];
  const directTargets = [];

  for (const item of rawResults) {
    if (seenUrls.has(item.url)) continue;
    seenUrls.add(item.url);

    const domain = safeHostname(item.url);
    if (!domain) continue;
    if (isBlocked(domain)) continue;

    if (isDirectory(domain)) {
      const count = dirPageCount.get(domain) ?? 0;
      if (count >= MAX_DIR_PAGES) continue;
      dirPageCount.set(domain, count + 1);
      dirTargets.push({ url: item.url, domain, snippet: item.snippet, isDirectory: true });
    } else {
      if (seenDirectDomains.has(domain)) continue;
      seenDirectDomains.add(domain);
      directTargets.push({ url: item.url, domain, snippet: item.snippet, isDirectory: false });
    }
  }

  return [...dirTargets, ...directTargets];
}

// ═══════════════════════════════════════════════════════════════════════════════
// THE FULL EXTRACTION PIPELINE (web_search only — google_maps handled by Edge Fns)
// ═══════════════════════════════════════════════════════════════════════════════
async function executeExtractionPipeline(job) {
  const { id: preference_id, user_id, search_query } = job;

  console.log(`\n⚙️  Processing Search: "${search_query}"`);

  const seenUrls          = new Set();
  const seenDirectDomains = new Set();
  const dirPageCount      = new Map();
  const seenNames         = new Set();
  const allLeads          = [];
  const locationHint      = search_query.replace(/without websites?/gi, "").trim();

  // ══════════════════════════════════════════════════════════
  // PHASE 1 — QUERY EXPANSION
  // ══════════════════════════════════════════════════════════
  console.log("🧠 [PHASE 1] Expanding query…");

  const expandResult = await gptJson(
    [{
      role:    "user",
      content: `You are a lead-generation expert. Given the search intent below, produce a JSON
object with key "queries" containing exactly ${SUB_QUERY_COUNT} search strings.

COMPOSITION (follow strictly):

GROUP A — ${Math.ceil(SUB_QUERY_COUNT / 2)} queries targeting DIRECTORY / LISTING sites:
  Use these site names directly in queries:
  Justdial, Sulekha, Zomato, Magicpin, Tripadvisor, Yelp, Dineout, EazyDiner, Yellow Pages, LBB,
  IndiaMart, TradeIndia, Burrp, WhatShot, NearBuy, TimesCity
  Format examples:
    "justdial [business type] [city/area] contact phone"
    "zomato [business type] [city] restaurants list"
  VARY the area/neighbourhood in each query. Use DIFFERENT directories in each.

GROUP B — ${Math.floor(SUB_QUERY_COUNT / 2)} queries targeting OFFICIAL / DIRECT business websites:
  Use specific neighbourhoods, business names if known, "official site", "contact us",
  "phone number", "email", "address".
  VARY keywords and areas.

Output ONLY the JSON object. No extra keys. No markdown.
Search intent: "${search_query}"`,
    }],
    "phase-1-expand",
  );

  const subQueries = (
    expandResult?.queries ??
    expandResult?.searchQueries ??
    (expandResult ? Object.values(expandResult)[0] : null) ??
    [search_query]
  ).slice(0, SUB_QUERY_COUNT);

  console.log(`✨ [PHASE 1] ${subQueries.length} sub-queries generated.`);

  // ══════════════════════════════════════════════════════════
  // PHASE 2 — DISCOVERY
  // ══════════════════════════════════════════════════════════
  console.log("🔍 [PHASE 2] Running bulk URL discovery…");

  const rawResults = await runDiscovery(subQueries);

  console.log(`🌐 [PHASE 2] ${rawResults.length} total raw URLs collected.`);

  // ══════════════════════════════════════════════════════════
  // PHASE 3 — FILTER, DEDUP & SORT
  // ══════════════════════════════════════════════════════════
  console.log("🧹 [PHASE 3] Filtering and deduplicating…");

  const targets = filterAndDedup(rawResults, seenUrls, seenDirectDomains, dirPageCount);

  const dirCount    = targets.filter((t) => t.isDirectory).length;
  const directCount = targets.filter((t) => !t.isDirectory).length;
  console.log(`🎯 [PHASE 3] ${targets.length} targets (${dirCount} directory + ${directCount} direct).`);

  // ══════════════════════════════════════════════════════════
  // PHASE 4 — EXTRACTION & SCORING
  // ══════════════════════════════════════════════════════════
  console.log(`⛏️  [PHASE 4] Extracting via Jina AI…`);

  await extractFromTargets({
    targets, search_query, preference_id, seenNames, allLeads,
  });

  console.log(`📦 [PHASE 4] Main extraction done: ${allLeads.length} leads.`);

  // ══════════════════════════════════════════════════════════
  // PHASE 4.5 — SERPER SNIPER: Resolve missing listing URLs
  // ══════════════════════════════════════════════════════════
  const leadsNeedingSniper = allLeads.filter((l) => l.needsSerper);
  if (leadsNeedingSniper.length > 0) {
    console.log(`🎯 [PHASE 4.5] Serper Sniper: resolving ${leadsNeedingSniper.length} missing listing URLs…`);

    await Promise.all(
      leadsNeedingSniper.map(async (item) => {
        if (!item.needsSerper) return;
        const { name, domain, location } = item.needsSerper;

        const exactUrl = await resolveExactUrlWithSerper(name, domain, location);

        if (exactUrl) {
          item.lead.lead_data.listing_url = exactUrl;
          item.lead.lead_data.best_link   = exactUrl || item.lead.lead_data.website || item.lead.lead_data.source_url;
          item.score += 8;
          console.log(`    🔫 Sniper hit: "${name}" → ${exactUrl}`);
        } else {
          console.log(`    💨 Sniper miss: "${name}" on ${domain}`);
        }
      })
    );

    console.log(`✅ [PHASE 4.5] Serper Sniper complete.`);
  }

  // ══════════════════════════════════════════════════════════
  // PHASE 4.75 — ENRICHMENT: Find website/social for Tier 3
  // ══════════════════════════════════════════════════════════
  const tier3Leads = allLeads.filter((l) => l.needsEnrichment);
  if (tier3Leads.length > 0) {
    console.log(`🔍 [PHASE 4.75] Enriching ${tier3Leads.length} Tier 3 leads (finding websites/socials)…`);

    let upgradedToTier1 = 0;
    let upgradedToTier2 = 0;

    await Promise.all(
      tier3Leads.map(async (item) => {
        const { website: foundWebsite, socialLinks } = await resolveWebsiteAndSocials(
          item.businessName,
          locationHint,
        );

        const ld = item.lead.lead_data;

        if (foundWebsite && !isRealBusinessWebsite(ld.website)) {
          ld.website   = foundWebsite;
          ld.best_link = ld.listing_url || foundWebsite || ld.source_url;

          const newHost = safeHostname(foundWebsite);
          if (newHost) {
            item.lead.domain = newHost;
          }
        }

        if (socialLinks.length > 0) {
          const existing = ld.social_media_links ?? [];
          const merged   = [...new Set([...existing, ...socialLinks])];
          ld.social_media_links = merged;

          for (const url of socialLinks) {
            if (!ld.instagram_url && url.includes("instagram.com")) ld.instagram_url = url;
            if (!ld.facebook_url && url.includes("facebook.com"))   ld.facebook_url = url;
            if (!ld.twitter_url && (url.includes("twitter.com") || url.includes("x.com"))) ld.twitter_url = url;
            if (!ld.youtube_url && url.includes("youtube.com"))     ld.youtube_url = url;
          }
        }

        const allSocials = ld.social_media_links ?? [];
        const { tier: newTier, tierLabel: newLabel } = classifyLeadTier(ld.website, allSocials);

        if (newTier !== item.tier) {
          item.score -= TIER_SCORE_BONUS[item.tier] ?? 0;
          item.score += TIER_SCORE_BONUS[newTier] ?? 0;

          if (newTier === TIER_WEBSITE) upgradedToTier1++;
          if (newTier === TIER_SOCIAL_MEDIA) upgradedToTier2++;

          console.log(`    ⬆️  "${item.businessName}" upgraded: ${item.tierLabel} → ${newLabel}`);
        }

        item.tier      = newTier;
        item.tierLabel  = newLabel;
        ld.tier         = newTier;
        ld.tier_label   = newLabel;
      })
    );

    console.log(`✅ [PHASE 4.75] Enrichment complete: ${upgradedToTier1} → Tier 1, ${upgradedToTier2} → Tier 2.`);
  }

  // ══════════════════════════════════════════════════════════
  // PHASE 4.9 — SHORTFALL RECOVERY
  // ══════════════════════════════════════════════════════════
  if (allLeads.length < FINAL_OUTPUT_SIZE) {
    console.log(`⚠️  [PHASE 4.9] Only ${allLeads.length} leads — need ${FINAL_OUTPUT_SIZE}. Running recovery…`);

    const recoveryResult = await gptJson(
      [{
        role: "user",
        content: `You are a lead-generation expert. I need MORE leads — I only found ${allLeads.length} out of ${FINAL_OUTPUT_SIZE} needed.

Original search: "${search_query}"
Already used these queries (DO NOT repeat any): ${JSON.stringify(subQueries)}

Generate exactly 8 NEW, COMPLETELY DIFFERENT search queries targeting:
- DIFFERENT neighbourhoods/areas/cities than the ones above
- DIFFERENT directory sites (try: IndiaMart, TradeIndia, Sulekha, Magicpin, LBB, NearBuy, WhatShot, TimesCity, Burrp, So.City)
- DIFFERENT keyword angles ("near me", "top rated", "best", "reviews", "contact number", "phone number", "list of", "directory")
- Try broader geographic scope if original was too narrow

Return ONLY JSON: { "queries": ["...", "..."] }`,
      }],
      "phase-4.9-recovery",
    );

    const recoveryQueries = (recoveryResult?.queries ?? []).slice(0, 8);

    if (recoveryQueries.length > 0) {
      console.log(`  🔄 ${recoveryQueries.length} recovery queries generated.`);

      const recoveryRaw = await runDiscovery(recoveryQueries);
      console.log(`  🌐 ${recoveryRaw.length} recovery URLs found.`);

      const recoveryTargets = filterAndDedup(
        recoveryRaw, seenUrls, seenDirectDomains, dirPageCount,
      );
      console.log(`  🎯 ${recoveryTargets.length} new targets after dedup.`);

      if (recoveryTargets.length > 0) {
        await extractFromTargets({
          targets: recoveryTargets,
          search_query,
          preference_id,
          seenNames,
          allLeads,
        });
      }

      console.log(`✅ [PHASE 4.9] Recovery complete. Total: ${allLeads.length} leads.`);
    }
  }

  // ══════════════════════════════════════════════════════════
  // FINAL SORT — Tier first, then score within tier
  // ══════════════════════════════════════════════════════════
  allLeads.sort((a, b) => {
    if (a.tier !== b.tier) return a.tier.localeCompare(b.tier);
    return b.score - a.score;
  });

  const finalLeads = allLeads.slice(0, FINAL_OUTPUT_SIZE).map((x) => x.lead);

  const tier1Count = allLeads.filter((l) => l.tier === TIER_WEBSITE).length;
  const tier2Count = allLeads.filter((l) => l.tier === TIER_SOCIAL_MEDIA).length;
  const tier3Count = allLeads.filter((l) => l.tier === TIER_NONE).length;

  console.log(`📊 [RANKING] Tier breakdown:`);
  console.log(`   🥇 Tier 1 (Has Website):      ${tier1Count}`);
  console.log(`   🥈 Tier 2 (Has Social Media):  ${tier2Count}`);
  console.log(`   🥉 Tier 3 (Neither):           ${tier3Count}`);
  console.log(`✅ [FINAL] ${allLeads.length} extracted → top ${finalLeads.length} selected.`);

  // ══════════════════════════════════════════════════════════
  // PHASE 5 — SAVE TO DATABASE
  // ══════════════════════════════════════════════════════════
  if (finalLeads.length > 0) {
    console.log("💾 [PHASE 5] Saving leads…");

    const domainSeen = new Set();
    const dedupedLeads = finalLeads.filter((lead) => {
      const key = lead.domain;
      if (domainSeen.has(key)) return false;
      domainSeen.add(key);
      return true;
    });

    console.log(`  ${finalLeads.length} leads → ${dedupedLeads.length} after domain dedup.`);

    // ── SANITIZE: Ensure every lead has a valid best_link URL ──────
    // This prevents the frontend "URL using bad/illegal format" error.
    for (const lead of dedupedLeads) {
      const ld = lead.lead_data;

      // Rebuild best_link from scratch — guaranteed valid
      ld.best_link = ld.listing_url
        || ld.website
        || ld.instagram_url
        || ld.facebook_url
        || ld.twitter_url
        || ld.youtube_url
        || ld.other_social_url
        || ld.source_url
        || "";

      // Validate: if best_link is empty or malformed, use safe fallback
      try {
        if (!ld.best_link) throw new Error("empty");
        new URL(ld.best_link);
      } catch {
        ld.best_link = ld.source_url
          || `https://www.google.com/search?q=${encodeURIComponent(ld.company_name || "business")}`;
      }

      // Clean website field — if present but malformed, clear it
      if (ld.website) {
        try { new URL(ld.website); }
        catch { ld.website = ""; }
      }

      // Clean listing_url — if present but malformed, clear it
      if (ld.listing_url) {
        try { new URL(ld.listing_url); }
        catch { ld.listing_url = ""; }
      }
    }

    const { data: inserted, error: leadsError } = await supabase
      .from("leads")
      .upsert(dedupedLeads, { onConflict: "domain" })
      .select("id");

    if (leadsError) console.error("  Leads upsert error:", leadsError.message);

    if (inserted && inserted.length > 0) {
      const junction = inserted.map((l) => ({
        user_id,
        lead_id: l.id,
      }));

      const { error: junctionError } = await supabase
        .from("user_leads")
        .upsert(junction, { onConflict: "user_id,lead_id" });

      if (junctionError) {
        console.error("  user_leads error:", junctionError.message);
      } else {
        console.log(`💾 [SUCCESS] ${inserted.length} leads saved & linked to user.`);
      }
    }
  } else {
    console.log("⚠️  No leads extracted.");
  }

  return {
    total_extracted: allLeads.length,
    leads_saved:     finalLeads.length,
    tier_breakdown: { tier1Count, tier2Count, tier3Count },
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// ZOMBIE JOB CLEANUP
//
// Catches jobs where the Edge Function crashed before setting resolved_pipeline.
// These jobs have: status='pending' AND resolved_pipeline=NULL for over 60 seconds.
// Rescues them by defaulting to web_search so this worker picks them up.
// ═══════════════════════════════════════════════════════════════════════════════
async function cleanupZombieJobs() {
  try {
    const cutoff = new Date(Date.now() - 60 * 1000).toISOString();

    const { data: zombies, error } = await supabase
      .from("lead_preferences")
      .select("id, search_query, created_at")
      .eq("status", "pending")
      .is("resolved_pipeline", null)
      .lt("created_at", cutoff)
      .limit(5);

    if (error || !zombies || zombies.length === 0) return;

    console.log(`🧟 [ZOMBIE CLEANUP] Found ${zombies.length} stuck jobs. Rescuing…`);

    for (const zombie of zombies) {
      const { error: updateErr } = await supabase
        .from("lead_preferences")
        .update({ resolved_pipeline: "web_search" })
        .eq("id", zombie.id)
        .eq("status", "pending")
        .is("resolved_pipeline", null);

      if (!updateErr) {
        console.log(`   🧟→🔍 Rescued: "${zombie.search_query}" (${zombie.id}) → web_search`);
      }
    }
  } catch (err) {
    console.error("🧟 Zombie cleanup error:", err.message);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// THE DATABASE POLLER (STRICT FIFO QUEUE)
//
// CHANGED FROM ORIGINAL:
//   1. Polling query now filters: .eq("resolved_pipeline", "web_search")
//      → Google Maps jobs never reach this worker (Edge Functions handle them)
//   2. Idle branch calls cleanupZombieJobs() before sleeping
//      → Rescues jobs where Edge Function crashed (resolved_pipeline stuck NULL)
// ═══════════════════════════════════════════════════════════════════════════════
async function startWorker() {
  console.log("══════════════════════════════════════════════════════════");
  console.log("👷 Backend Worker Online. Polling for 'pending' web_search jobs...");
  console.log("══════════════════════════════════════════════════════════");

  while (true) {
    try {
      // ── CHANGED: Only pick up web_search jobs ──────────────────────
      const { data: jobs, error: fetchError } = await supabase
        .from("lead_preferences")
        .select("*")
        .eq("status", "pending")
        .eq("resolved_pipeline", "web_search")   // ← NEW: skip google_maps jobs
        .order("created_at", { ascending: true })
        .limit(1);

      if (fetchError) throw fetchError;

      if (jobs && jobs.length > 0) {
        const job = jobs[0];
        console.log(`\n📥 Grabbed pending job: ${job.id}`);

        const { data: lockedJob, error: lockError } = await supabase
          .from("lead_preferences")
          .update({ status: "processing", started_at: new Date().toISOString() })
          .eq("id", job.id)
          .eq("status", "pending")
          .select()
          .single();

        if (lockError || !lockedJob) {
          console.log("   ⚠️ Job lock failed (maybe already processing). Skipping.");
          continue;
        }

        try {
          const result = await executeExtractionPipeline(lockedJob);

          await supabase
            .from("lead_preferences")
            .update({ status: "completed", completed_at: new Date().toISOString() })
            .eq("id", lockedJob.id);

          console.log(`✅ Job Completed Successfully. ${result.leads_saved} leads saved.`);
        } catch (pipelineError) {
          console.error(`❌ Pipeline Crashed:`, pipelineError.message);

          await supabase
            .from("lead_preferences")
            .update({ status: "failed", error_message: pipelineError.message })
            .eq("id", lockedJob.id);
        }
      } else {
        // ── CHANGED: Run zombie cleanup when idle ──────────────────
        await cleanupZombieJobs();
        await sleep(5000);
      }
    } catch (globalError) {
      console.error("⚠️ Database connection error:", globalError.message);
      await sleep(10000);
    }
  }
}

// Boot up!
startWorker();