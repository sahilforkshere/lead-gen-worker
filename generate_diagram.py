"""
Lead-Alert B2B Data Engine — End-to-End Architecture Diagram Generator
Generates a detailed PNG image showing every step from user query to results.
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.patheffects as pe
import textwrap

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

FIG_W, FIG_H = 36, 52
DPI = 180
BG_COLOR = "#0B0F1A"
GRID_COLOR = "#141B2D"

# Color palette
C = {
    "frontend":   "#6C5CE7",
    "database":   "#00B894",
    "worker":     "#FDCB6E",
    "api_ai":     "#E17055",
    "api_search": "#0984E3",
    "api_read":   "#00CEC9",
    "api_sniper": "#D63031",
    "result":     "#55EFC4",
    "arrow":      "#636E72",
    "arrow_hot":  "#E17055",
    "text":       "#DFE6E9",
    "text_dim":   "#636E72",
    "border":     "#2D3436",
    "phase_bg":   "#141B2D",
    "tier1":      "#55EFC4",
    "tier2":      "#FDCB6E",
    "tier3":      "#D63031",
    "white":      "#FFFFFF",
}


def draw_box(ax, x, y, w, h, label, sublabel="", color="#6C5CE7",
             fontsize=13, sublabel_size=9.5, alpha=0.92, radius=0.02,
             icon="", bold=True, border_width=2.0):
    """Draw a rounded rectangle with label and optional sublabel."""
    box = FancyBboxPatch(
        (x - w/2, y - h/2), w, h,
        boxstyle=f"round,pad=0.015,rounding_size={radius}",
        facecolor=color, edgecolor=lighten(color, 0.3),
        linewidth=border_width, alpha=alpha, zorder=3,
    )
    ax.add_patch(box)

    # Main label
    full_label = f"{icon}  {label}" if icon else label
    weight = "bold" if bold else "normal"
    txt_y = y + (0.12 if sublabel else 0)
    ax.text(x, txt_y, full_label, ha="center", va="center",
            fontsize=fontsize, color=C["white"], fontweight=weight,
            zorder=5, path_effects=[pe.withStroke(linewidth=2, foreground="#00000088")])

    # Sublabel
    if sublabel:
        wrapped = "\n".join(textwrap.wrap(sublabel, width=48))
        ax.text(x, y - 0.18, wrapped, ha="center", va="center",
                fontsize=sublabel_size, color="#B2BEC3", fontstyle="italic",
                zorder=5, linespacing=1.4)


def draw_arrow(ax, x1, y1, x2, y2, color="#636E72", style="-|>",
               lw=2.0, connectionstyle="arc3,rad=0.0", linestyle="-"):
    """Draw a curved or straight arrow between two points."""
    arrow = FancyArrowPatch(
        (x1, y1), (x2, y2),
        arrowstyle=style, color=color,
        linewidth=lw, connectionstyle=connectionstyle,
        mutation_scale=18, zorder=2, linestyle=linestyle,
    )
    ax.add_patch(arrow)


def draw_phase_banner(ax, y, label, number, color="#FDCB6E"):
    """Draw a phase separator banner."""
    ax.axhline(y=y, xmin=0.03, xmax=0.97, color=color, linewidth=1.2,
               alpha=0.25, zorder=1, linestyle="--")
    ax.text(0.5, y + 0.28, f"━━━  PHASE {number}: {label}  ━━━",
            ha="center", va="center", fontsize=14, color=color,
            fontweight="bold", zorder=5, alpha=0.9,
            path_effects=[pe.withStroke(linewidth=3, foreground="#00000066")])


def draw_data_flow_label(ax, x, y, text, color="#636E72", fontsize=8.5, rotation=0):
    """Draw a small label on an arrow to show data flow."""
    ax.text(x, y, text, ha="center", va="center", fontsize=fontsize,
            color=color, zorder=6, rotation=rotation, alpha=0.85,
            bbox=dict(boxstyle="round,pad=0.15", facecolor=BG_COLOR,
                      edgecolor="none", alpha=0.85))


def lighten(hex_color, factor=0.3):
    """Lighten a hex color."""
    hex_color = hex_color.lstrip("#")
    r, g, b = int(hex_color[:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
    r = min(255, int(r + (255 - r) * factor))
    g = min(255, int(g + (255 - g) * factor))
    b = min(255, int(b + (255 - b) * factor))
    return f"#{r:02x}{g:02x}{b:02x}"


def darken(hex_color, factor=0.3):
    """Darken a hex color."""
    hex_color = hex_color.lstrip("#")
    r, g, b = int(hex_color[:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
    r = max(0, int(r * (1 - factor)))
    g = max(0, int(g * (1 - factor)))
    b = max(0, int(b * (1 - factor)))
    return f"#{r:02x}{g:02x}{b:02x}"


# ═══════════════════════════════════════════════════════════════════════════════
# BUILD THE DIAGRAM
# ═══════════════════════════════════════════════════════════════════════════════

fig, ax = plt.subplots(1, 1, figsize=(FIG_W, FIG_H), dpi=DPI)
fig.patch.set_facecolor(BG_COLOR)
ax.set_facecolor(BG_COLOR)
ax.set_xlim(-1, 37)
ax.set_ylim(-2, 52)
ax.set_aspect("equal")
ax.axis("off")

# ─── TITLE ─────────────────────────────────────────────────────────────────
ax.text(18, 51, "LEAD-ALERT  B2B  DATA  ENGINE", ha="center", va="center",
        fontsize=28, color=C["white"], fontweight="bold", zorder=5,
        path_effects=[pe.withStroke(linewidth=4, foreground="#00000088")])
ax.text(18, 50.2, "End-to-End Architecture  ·  Query → Worker → APIs → Enriched Leads",
        ha="center", va="center", fontsize=13, color=C["text_dim"], zorder=5)

# ═══════════════════════════════════════════════════════════════════════════════
# ROW 0 — USER & FRONTEND (Y = 48)
# ═══════════════════════════════════════════════════════════════════════════════

Y = 48.0

draw_box(ax, 7, Y, 7.5, 1.6, "USER", "Enters search query in dashboard",
         color="#2D3436", fontsize=15, icon="")

draw_box(ax, 18, Y, 9.5, 1.6, "Next.js Frontend",
         'supabase.rpc("submit_search", { query_text })',
         color=C["frontend"], fontsize=14, icon="")

draw_box(ax, 30, Y, 8.5, 1.6, "Supabase Realtime",
         "Subscribes: lead_preferences changes",
         color=C["database"], fontsize=12, icon="")

draw_arrow(ax, 10.75, Y, 13.25, Y, color=C["arrow_hot"], lw=2.5)
draw_data_flow_label(ax, 12, Y + 0.35, "search query")

draw_arrow(ax, 22.75, Y, 25.75, Y, color=C["database"], lw=2.0)
draw_data_flow_label(ax, 24.2, Y + 0.35, "subscribe()")


# ═══════════════════════════════════════════════════════════════════════════════
# ROW 1 — DATABASE INSERT (Y = 45)
# ═══════════════════════════════════════════════════════════════════════════════

Y = 45.2

draw_box(ax, 18, Y, 12, 2.0, "submit_search()  →  lead_preferences",
         'INSERT ... ON CONFLICT (user_id) DO UPDATE\n'
         'SET status = "pending", clears old timestamps\n'
         'DELETE FROM user_leads WHERE user_id = ...',
         color=C["database"], fontsize=13, icon="", sublabel_size=9)

draw_arrow(ax, 18, 47.2, 18, 46.2, color=C["frontend"], lw=2.5)
draw_data_flow_label(ax, 19.2, 46.7, "RPC call")


# ═══════════════════════════════════════════════════════════════════════════════
# ROW 2 — WORKER POLLING (Y = 42)
# ═══════════════════════════════════════════════════════════════════════════════

draw_phase_banner(ax, 43, "WORKER POLLING LOOP", "0", color="#FDCB6E")

Y = 41.8

draw_box(ax, 18, Y, 14, 2.2, "Background Worker  (worker.js)",
         'while(true) {\n'
         '  SELECT * FROM lead_preferences\n'
         '  WHERE status = "pending" ORDER BY created_at ASC LIMIT 1\n'
         '  → UPDATE status = "processing"  (Optimistic Lock)\n'
         '  → executeExtractionPipeline(job)\n'
         '}',
         color=C["worker"], fontsize=14, icon="", sublabel_size=9)

draw_arrow(ax, 18, 44.2, 18, 42.9, color=C["database"], lw=2.5)
draw_data_flow_label(ax, 19.5, 43.55, "polls every 5s")


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — QUERY EXPANSION (Y = 38)
# ═══════════════════════════════════════════════════════════════════════════════

draw_phase_banner(ax, 39.5, "QUERY EXPANSION  (The Strategist)", "1", color=C["api_ai"])

Y = 37.8

draw_box(ax, 10, Y, 10, 2.0, "gptJson()",
         'model: "gpt-4o-mini"\n'
         'response_format: json_object\n'
         f'Generates {4} targeted sub-queries',
         color=C["api_ai"], fontsize=13, icon="", sublabel_size=9)

draw_box(ax, 26, Y, 10.5, 2.0, "Sub-Query Output",
         'Group A: Directory queries (Justdial, Zomato, Yelp...)\n'
         'Group B: Direct website queries ("official site", "contact us")\n'
         '4 total sub-queries',
         color="#2D3436", fontsize=12, sublabel_size=9)

draw_arrow(ax, 18, 40.7, 10, 38.8, color=C["api_ai"], lw=2.0)
draw_data_flow_label(ax, 13.5, 39.9, "search_query")

draw_arrow(ax, 15, Y, 20.75, Y, color=C["api_ai"], lw=2.0)
draw_data_flow_label(ax, 17.8, Y + 0.35, '{ queries: [...] }')


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — BULK DISCOVERY (Y = 34)
# ═══════════════════════════════════════════════════════════════════════════════

draw_phase_banner(ax, 35.8, "BULK DISCOVERY  (The Wide Net)", "2", color=C["api_search"])

Y = 34.0

draw_box(ax, 9, Y, 9, 2.0, "Parallel AI API",
         'POST /v1beta/search\n'
         'mode: "fast", max_results: 25\n'
         'excerpts: 4000 chars per result',
         color=C["api_search"], fontsize=12, icon="", sublabel_size=9)

draw_box(ax, 23, Y, 9, 2.0, "Tavily Search API",
         'POST /search\n'
         'search_depth: "advanced"\n'
         'max_results: 20 per query',
         color=C["api_search"], fontsize=12, icon="", sublabel_size=9)

draw_box(ax, 33.5, Y, 5, 1.4, "Raw URLs",
         "100-200+ URLs + snippets",
         color="#2D3436", fontsize=11, sublabel_size=8.5)

draw_arrow(ax, 9, 36.8, 9, 35, color=C["api_search"], lw=2.0,
           connectionstyle="arc3,rad=-0.15")
draw_arrow(ax, 23, 36.8, 23, 35, color=C["api_search"], lw=2.0,
           connectionstyle="arc3,rad=0.15")

draw_data_flow_label(ax, 7.5, 35.9, "sub-queries ×4")
draw_data_flow_label(ax, 24.5, 35.9, "sub-queries ×4")

draw_arrow(ax, 13.5, Y, 16, Y, color="#636E72", lw=1.5, linestyle="--")
draw_arrow(ax, 27.5, Y, 31, Y, color="#636E72", lw=1.5, linestyle="--")

# Promise.all label
ax.text(16, 35.7, "Promise.all([ parallelAI, tavily ])",
        ha="center", va="center", fontsize=9, color=C["worker"],
        fontweight="bold", zorder=5, fontstyle="italic")


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 3 — FILTER DEDUP ROUTE (Y = 30)
# ═══════════════════════════════════════════════════════════════════════════════

draw_phase_banner(ax, 32, "FILTER, DEDUP & ROUTE  (The Traffic Cop)", "3", color="#00CEC9")

Y = 30.3

draw_box(ax, 9, Y, 9, 2.2, "Filter Engine",
         'isBlocked(): Drop facebook, wikipedia, amazon...\n'
         'seenUrls: Exact URL dedup\n'
         'seenDirectDomains: One page per domain',
         color="#00CEC9", fontsize=12, icon="", sublabel_size=9)

draw_box(ax, 22, Y, 6.5, 1.4, "Directory Targets",
         'MAX_DIR_PAGES = 2 per domain',
         color=darken(C["api_search"], 0.2), fontsize=11, sublabel_size=8.5)

draw_box(ax, 31, Y, 6.5, 1.4, "Direct Targets",
         'One per unique domain',
         color=darken(C["api_read"], 0.3), fontsize=11, sublabel_size=8.5)

draw_arrow(ax, 9, 33, 9, 31.4, color="#00CEC9", lw=2.0)
draw_data_flow_label(ax, 10.3, 32.2, "raw URLs")

draw_arrow(ax, 13.5, 30.6, 18.75, 30.6, color="#636E72", lw=1.5)
draw_arrow(ax, 13.5, 30.0, 27.75, 30.0, color="#636E72", lw=1.5)

draw_data_flow_label(ax, 16, 30.95, "isDirectory() = true")
draw_data_flow_label(ax, 20.5, 29.6, "isDirectory() = false")


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 4 — EXTRACTION & CLASSIFICATION (Y = 26)
# ═══════════════════════════════════════════════════════════════════════════════

draw_phase_banner(ax, 28.2, "EXTRACTION & CLASSIFICATION  (The Deep Reader)", "4", color=C["api_ai"])

Y = 26.2

# 4A — Jina
draw_box(ax, 7, Y, 8.5, 2.2, "fetchPageWithJina(url)",
         'GET https://r.jina.ai/{url}\n'
         'X-Return-Format: markdown\n'
         'Preserves all <a href="..."> links\n'
         'Fallback: use search snippet if < 200 chars',
         color=C["api_read"], fontsize=11, icon="", sublabel_size=8.5)

# 4B — GPT Extract
draw_box(ax, 20, Y, 10, 2.4, "gptJson() — Extraction",
         'Reads Jina Markdown → extracts per business:\n'
         'company_name, phone, email, address, website,\n'
         'listing_url, instagram_url, facebook_url,\n'
         'twitter_url, youtube_url, other_social_url,\n'
         'description  ·  Batches of EXTRACT_BATCH_SIZE=5',
         color=C["api_ai"], fontsize=11, icon="", sublabel_size=8.5)

# 4C — Tier Classification
draw_box(ax, 33, Y, 6.5, 2.2, "classifyLeadTier()",
         'Tier 1: Has website → +50\n'
         'Tier 2: Social media → +25\n'
         'Tier 3: Neither → +0\n'
         'isRealBusinessWebsite()',
         color="#2D3436", fontsize=11, sublabel_size=8.5)

draw_arrow(ax, 7, 29.2, 7, 27.3, color=C["api_read"], lw=2.0)
draw_arrow(ax, 22, 29.2, 22, 27.4, color=C["api_ai"], lw=2.0)

draw_data_flow_label(ax, 5.5, 28.3, "targets[]")
draw_data_flow_label(ax, 23.4, 28.3, "content (12000 chars)")

draw_arrow(ax, 11.25, Y, 15, Y, color=C["api_read"], lw=2.0)
draw_data_flow_label(ax, 13, Y + 0.4, "markdown")

draw_arrow(ax, 25, Y, 29.75, Y, color=C["api_ai"], lw=2.0)
draw_data_flow_label(ax, 27.3, Y + 0.4, "businesses[]")

# Scoring box below
Y_score = 23.5
draw_box(ax, 18, Y_score, 14, 1.6, "Scoring Engine",
         'score = name(10) + phone(30) + email(30) + address(10) + listing(8) + desc(5) + tierBonus\n'
         'seenNames dedup via normaliseName()  ·  domainKey for DB dedup',
         color=darken(C["worker"], 0.3), fontsize=11, icon="", sublabel_size=8.5)

draw_arrow(ax, 20, 25, 18, 24.3, color="#636E72", lw=1.5)
draw_arrow(ax, 33, 25.1, 25, 24.3, color="#636E72", lw=1.5)


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 4.5 — SERPER SNIPER (Y = 20.5)
# ═══════════════════════════════════════════════════════════════════════════════

draw_phase_banner(ax, 21.8, "URL RESOLUTION  (The Sniper)", "4.5", color=C["api_sniper"])

Y = 20.0

draw_box(ax, 11, Y, 11, 2.0, "resolveExactUrlWithSerper()",
         'For directory leads missing listing_url:\n'
         'query: "BusinessName location site:justdial.com"\n'
         'POST https://google.serper.dev/search  ·  num: 3\n'
         'Hit → listing_url updated, score += 8',
         color=C["api_sniper"], fontsize=11, icon="", sublabel_size=8.5)

draw_box(ax, 27, Y, 8, 1.4, "allLeads[] updated",
         'listing_url + best_link patched',
         color="#2D3436", fontsize=11, sublabel_size=8.5)

draw_arrow(ax, 18, 22.7, 11, 21, color=C["api_sniper"], lw=2.0)
draw_data_flow_label(ax, 14, 22, "needsSerper = true")

draw_arrow(ax, 16.5, Y, 23, Y, color=C["api_sniper"], lw=2.0)
draw_data_flow_label(ax, 19.8, Y + 0.35, "exact URL")


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 4.75 — ENRICHMENT (Y = 16.5)
# ═══════════════════════════════════════════════════════════════════════════════

draw_phase_banner(ax, 17.8, "TIER 3 ENRICHMENT  (Upgrade Engine)", "4.75", color=C["tier2"])

Y = 16.0

draw_box(ax, 11, Y, 11, 2.2, "resolveWebsiteAndSocials()",
         'For Tier 3 leads (no website, no socials):\n'
         'query: "BusinessName location official website"\n'
         'POST https://google.serper.dev/search  ·  num: 8\n'
         'Checks organic results + knowledgeGraph.profiles\n'
         'Discovers hidden websites & social pages',
         color=C["tier2"], fontsize=11, icon="", sublabel_size=8.5)

draw_box(ax, 27.5, Y, 8.5, 1.8, "Tier Re-Classification",
         'classifyLeadTier() re-run\n'
         'Score adjusted: −old bonus + new bonus\n'
         'Tier 3 → Tier 1 or Tier 2',
         color="#2D3436", fontsize=11, sublabel_size=8.5)

draw_arrow(ax, 11, 19, 11, 17.1, color=C["tier2"], lw=2.0)
draw_data_flow_label(ax, 9.3, 18, "needsEnrichment = true")

draw_arrow(ax, 16.5, Y, 23.25, Y, color=C["tier2"], lw=2.0)
draw_data_flow_label(ax, 19.8, Y + 0.35, "website + socialLinks[]")

# Tier legend
for i, (tier_name, tier_color, tier_desc) in enumerate([
    ("TIER 1", C["tier1"], "Has Website (+50)"),
    ("TIER 2", C["tier2"], "Social Media (+25)"),
    ("TIER 3", C["tier3"], "Neither (+0)"),
]):
    tx = 34
    ty = Y + 0.8 - i * 0.65
    ax.plot(tx - 0.4, ty, "s", color=tier_color, markersize=10, zorder=5)
    ax.text(tx, ty, f"{tier_name}: {tier_desc}", ha="left", va="center",
            fontsize=9, color=tier_color, zorder=5, fontweight="bold")


# ═══════════════════════════════════════════════════════════════════════════════
# FINAL SORT (Y = 12.5)
# ═══════════════════════════════════════════════════════════════════════════════

draw_phase_banner(ax, 13.8, "FINAL SORT & SELECT", "—", color=C["result"])

Y = 12.2

draw_box(ax, 18, Y, 14, 1.8, "Sort & Slice",
         'Primary: tier (tier_1 < tier_2 < tier_3 lexical)\n'
         'Secondary: score DESC within same tier\n'
         'allLeads.slice(0, FINAL_OUTPUT_SIZE=20)',
         color=darken(C["result"], 0.4), fontsize=12, icon="", sublabel_size=9)

draw_arrow(ax, 18, 14.9, 18, 13.1, color=C["result"], lw=2.5)


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 5 — DATABASE UPSERT (Y = 9)
# ═══════════════════════════════════════════════════════════════════════════════

draw_phase_banner(ax, 10.3, "DATABASE UPSERT  (The Vault)", "5", color=C["database"])

Y = 8.5

draw_box(ax, 10, Y, 10.5, 2.2, "leads Table — Upsert",
         'Domain dedup: domainSeen Set\n'
         'supabase.from("leads").upsert(dedupedLeads,\n'
         '  { onConflict: "domain" }).select("id")\n'
         'Stores: lead_data JSONB (name, phone, tiers...)',
         color=C["database"], fontsize=11, icon="", sublabel_size=8.5)

draw_box(ax, 26, Y, 10, 1.8, "user_leads Junction",
         'supabase.from("user_leads").upsert(\n'
         '  { user_id, lead_id },\n'
         '  { onConflict: "user_id,lead_id" })',
         color=C["database"], fontsize=11, icon="", sublabel_size=8.5)

draw_arrow(ax, 18, 11.3, 10, 9.6, color=C["database"], lw=2.0)
draw_arrow(ax, 15.25, Y, 21, Y, color=C["database"], lw=2.0)
draw_data_flow_label(ax, 18, Y + 0.35, "inserted[].id")


# ═══════════════════════════════════════════════════════════════════════════════
# ROW FINAL — STATUS UPDATE & REALTIME PUSH (Y = 5)
# ═══════════════════════════════════════════════════════════════════════════════

draw_phase_banner(ax, 6.3, "COMPLETION & DELIVERY", "—", color=C["frontend"])

Y = 4.8

draw_box(ax, 10, Y, 10, 1.6, 'status → "completed"',
         'lead_preferences.update({ status: "completed",\n'
         '  completed_at: new Date() })',
         color=C["database"], fontsize=11, icon="", sublabel_size=8.5)

draw_box(ax, 26, Y, 9, 1.6, "Supabase Realtime Push",
         'postgres_changes event: UPDATE\n'
         'filter: user_id=eq.{userId}',
         color=lighten(C["database"], 0.2), fontsize=11, icon="", sublabel_size=8.5)

draw_arrow(ax, 15, Y, 21.5, Y, color=C["database"], lw=2.5)
draw_data_flow_label(ax, 18.2, Y + 0.35, "triggers realtime")

draw_arrow(ax, 10, 7.7, 10, 5.6, color=C["database"], lw=2.0)

# ─── FRONTEND RECEIVES ────────────────────────────────────────────────

Y = 2.5

draw_box(ax, 18, Y, 14, 2.0, "Next.js Frontend — Dashboard Renders",
         'payload.new.status === "completed"\n'
         '→ Fetch leads via user_leads JOIN leads\n'
         '→ Display Tier 1 / Tier 2 / Tier 3 leads with all contact data\n'
         '→ User sees enriched leads in real-time',
         color=C["frontend"], fontsize=13, icon="", sublabel_size=9.5)

draw_arrow(ax, 26, 4, 26, 3.5, color=C["frontend"], lw=2.5)
draw_arrow(ax, 22, 3.5, 20, 3.5, color=C["frontend"], lw=2.0,
           connectionstyle="arc3,rad=0.3")

# ─── LEGEND ────────────────────────────────────────────────────────────

legend_items = [
    ("Frontend (Next.js)", C["frontend"]),
    ("Database (Supabase PG)", C["database"]),
    ("Worker (Node.js)", C["worker"]),
    ("AI APIs (OpenAI)", C["api_ai"]),
    ("Search APIs (Parallel/Tavily)", C["api_search"]),
    ("Reader API (Jina AI)", C["api_read"]),
    ("Sniper API (Serper.dev)", C["api_sniper"]),
]

for i, (label, color) in enumerate(legend_items):
    lx = 1 + (i % 4) * 9
    ly = 0.2 if i < 4 else -0.6
    ax.plot(lx, ly, "s", color=color, markersize=12, zorder=5)
    ax.text(lx + 0.5, ly, label, ha="left", va="center",
            fontsize=9.5, color=C["text"], zorder=5)


# ═══════════════════════════════════════════════════════════════════════════════
# SAVE
# ═══════════════════════════════════════════════════════════════════════════════

plt.tight_layout(pad=0.5)
output_path = "/mnt/user-data/outputs/lead-alert-architecture.png"
fig.savefig(output_path, dpi=DPI, facecolor=BG_COLOR, bbox_inches="tight", pad_inches=0.3)
plt.close()
print(f"✅ Saved to {output_path}")
