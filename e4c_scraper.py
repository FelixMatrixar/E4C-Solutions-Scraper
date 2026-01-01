"""
E4C Solutions Library Scraper
==============================
Scrapes all product pages from engineeringforchange.org/solutions-library/
and saves structured JSON per solution.

Usage:
    pip install requests beautifulsoup4 lxml
    python e4c_scraper.py            # full run
    python e4c_scraper.py retry      # retry failed URLs only
    python e4c_scraper.py merge      # re-merge individual files without re-scraping
    python e4c_scraper.py build-es   # build Elasticsearch bulk import NDJSON

Output:
    e4c_solutions/          one JSON file per product  (slug.json)
    e4c_solutions_all.json  merged full dataset
    e4c_scrape_errors.json  failed URLs for retry
    e4c_product_links.json  cached discovered URLs (delete to re-discover)

Key design decisions vs original scraper:
    - Pages are fully server-side rendered; no JS/Playwright needed.
    - Tabs are visual anchors only; all content is on one HTML page.
    - Content is parsed with label-aware extraction, not CSS class guessing.
    - Resume-safe: skips already-scraped slugs on re-run.
    - Rate-limited per thread (DELAY seconds) to be polite.
"""

import requests
from bs4 import BeautifulSoup, NavigableString, Tag
import json
import time
import os
import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL       = "https://www.engineeringforchange.org"
LIBRARY_URL    = f"{BASE_URL}/solutions-library/"
OUTPUT_DIR     = Path("e4c_solutions")
MERGED_OUTPUT  = Path("e4c_solutions_all.json")
ERRORS_OUTPUT  = Path("e4c_scrape_errors.json")
LINKS_CACHE    = Path("e4c_product_links.json")

DELAY          = 1.2    # seconds between requests per thread
MAX_WORKERS    = 3      # concurrent threads -- keep low, be polite
MAX_RETRIES    = 3
RETRY_DELAY    = 6.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; E4C-Research-Scraper/1.0; "
        "Academic research; contact: research@example.com)"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("e4c_scrape.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

session = requests.Session()
session.headers.update(HEADERS)


def fetch(url: str, retries: int = MAX_RETRIES) -> Optional[BeautifulSoup]:
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=25)
            resp.raise_for_status()
            return BeautifulSoup(resp.content, "lxml")
        except requests.RequestException as exc:
            log.warning(f"Attempt {attempt}/{retries} failed [{url}]: {exc}")
            if attempt < retries:
                time.sleep(RETRY_DELAY * attempt)
    log.error(f"All retries exhausted: {url}")
    return None


# ---------------------------------------------------------------------------
# Link discovery
# ---------------------------------------------------------------------------

def discover_product_links() -> list:
    """
    Two-stage URL discovery:
      Stage 1: WordPress sitemap (fast, gets all URLs at once)
      Stage 2: BFS crawl via internal 'similar solutions' links (fallback)

    Each product page has ~10 hardcoded 'Explore similar solutions' links
    in the static HTML. BFS from a seed set covers all 1,034 products
    in roughly 100-150 page fetches.
    """
    links: set = set()

    # ── Stage 1: Try sitemap URLs ─────────────────────────────────────────────
    sitemap_candidates = [
        f"{BASE_URL}/wp-sitemap.xml",
        f"{BASE_URL}/sitemap.xml",
        f"{BASE_URL}/sitemap_index.xml",
    ]

    for sitemap_url in sitemap_candidates:
        log.info(f"Trying sitemap: {sitemap_url}")
        try:
            r = session.get(sitemap_url, timeout=20)
            if r.status_code != 200:
                continue

            # It's a sitemap index — find sub-sitemaps containing 'solutions'
            if "<sitemapindex" in r.text:
                import xml.etree.ElementTree as ET
                ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
                try:
                    root = ET.fromstring(r.text)
                    sub_urls = [
                        loc.text.strip()
                        for loc in root.findall(".//sm:loc", ns)
                        if loc.text and "solution" in loc.text.lower()
                    ]
                except ET.ParseError:
                    sub_urls = re.findall(r'<loc>(.*?solution.*?)</loc>', r.text)

                for sub_url in sub_urls:
                    log.info(f"  Sub-sitemap: {sub_url}")
                    r2 = session.get(sub_url, timeout=20)
                    if r2.status_code == 200:
                        found = re.findall(
                            r'<loc>(https?://[^<]*/solutions/product/[^<]*)</loc>',
                            r2.text
                        )
                        links.update(
                            (u.rstrip("/") + "/") for u in found
                        )
                    time.sleep(0.5)

            # It's a direct sitemap with product URLs
            else:
                found = re.findall(
                    r'<loc>(https?://[^<]*/solutions/product/[^<]*)</loc>',
                    r.text
                )
                links.update((u.rstrip("/") + "/") for u in found)

        except requests.RequestException as e:
            log.warning(f"  Sitemap error: {e}")
        time.sleep(0.5)

    # Also try numbered sub-sitemaps directly
    # WordPress pattern: /wp-sitemap-posts-solutions-1.xml, -2.xml, ...
    for page_n in range(1, 15):
        url = f"{BASE_URL}/wp-sitemap-posts-solutions-{page_n}.xml"
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 404:
                break
            if r.status_code == 200:
                found = re.findall(
                    r'<loc>(https?://[^<]*/solutions/product/[^<]*)</loc>',
                    r.text
                )
                if not found:
                    break
                log.info(f"  wp-sitemap page {page_n}: {len(found)} URLs")
                links.update((u.rstrip("/") + "/") for u in found)
            time.sleep(0.5)
        except requests.RequestException:
            break

    if links:
        log.info(f"Sitemap discovery: {len(links)} product URLs found.")

    # ── Stage 2: BFS via internal 'similar solutions' links ───────────────────
    # Always run this to fill gaps the sitemap may have missed.
    # Seed with known products + anything sitemap found.
    seeds = list(links) if links else []

    # Always include these known seeds
    known_seeds = [
        f"{BASE_URL}/solutions/product/splash-stations/",
        f"{BASE_URL}/solutions/product/lifestraw-family-1-0/",
        f"{BASE_URL}/solutions/product/jikojoy-charcoal-stove/",
        f"{BASE_URL}/solutions/product/kio-kit/",
        f"{BASE_URL}/solutions/product/wefarm/",
        f"{BASE_URL}/solutions/product/iz-southern-cross-windmill/",
        f"{BASE_URL}/solutions/product/safi-water-filters/",
        f"{BASE_URL}/solutions/product/mwater-explorer-mobile-app/",
    ]
    queue = list(set(seeds + known_seeds))
    visited: set = set()
    bfs_fetches = 0
    MAX_BFS_FETCHES = 200  # safety cap

    log.info(f"BFS crawl starting with {len(queue)} seeds...")

    while queue and bfs_fetches < MAX_BFS_FETCHES:
        url = queue.pop(0)
        normalized = url.rstrip("/") + "/"
        if normalized in visited:
            continue
        visited.add(normalized)
        links.add(normalized)
        bfs_fetches += 1

        soup = fetch(url)
        if not soup:
            continue

        # Extract all /solutions/product/ links from this page
        found_on_page = 0
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/solutions/product/" in href:
                full = urljoin(BASE_URL, href).rstrip("/") + "/"
                if full not in visited and full not in queue:
                    queue.append(full)
                    found_on_page += 1

        log.info(
            f"  BFS [{bfs_fetches}] {url.split('/')[-2]} "
            f"-> +{found_on_page} new | queue={len(queue)} | total={len(links)}"
        )
        time.sleep(DELAY)

    log.info(f"Discovery complete: {len(links)} unique product URLs.")
    return sorted(links)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def slug_from_url(url: str) -> str:
    return urlparse(url).path.rstrip("/").split("/")[-1]


def extract_breadcrumb(soup: BeautifulSoup) -> dict:
    """
    Extract sector / sub-sector / category from breadcrumb or page path.
    E4C breadcrumb: Solutions Library > Sector > Sub-sector > Category > Product
    """
    crumbs: list = []

    # Try explicit breadcrumb elements
    for sel in [
        "nav.breadcrumb a", ".breadcrumb a",
        "[aria-label='breadcrumb'] a", ".breadcrumbs a",
    ]:
        els = soup.select(sel)
        if els:
            crumbs = [
                clean(e.get_text()) for e in els
                if clean(e.get_text()).lower() not in ("solutions library", "home", "")
            ]
            break

    # Fallback: parse the unicode arrow separator E4C uses (⯈)
    if not crumbs:
        raw_text = clean(soup.get_text(" "))
        # Pattern: "Solutions Library ⯈ Water ⯈ Water treatment ⯈ Multibarrier water treatment ⯈ Product"
        pattern = re.search(
            r"Solutions Library\s*[⯈>]\s*(.+?)\s*[⯈>]\s*(.+?)\s*[⯈>]\s*(.+?)(?:\s*[⯈>]\s*(.+?))?(?:\n|\s{3,}|$)",
            raw_text,
        )
        if pattern:
            crumbs = [clean(g) for g in pattern.groups() if g and clean(g)]

    result = {}
    fields = ["sector", "sub_sector", "category", "sub_category"]
    for i, field in enumerate(fields):
        if i < len(crumbs):
            result[field] = crumbs[i]
    return result


def extract_attribution(soup: BeautifulSoup) -> dict:
    """Extract Developed By / Tested By / Content Partners."""
    result: dict = {"developed_by": [], "tested_by": [], "content_partners": []}
    for section_text, key in [
        ("Developed By", "developed_by"),
        ("Tested By", "tested_by"),
        ("Content Partners", "content_partners"),
    ]:
        heading = None
        for tag in soup.find_all(["h3", "h4", "h5", "dt", "strong", "b", "p"]):
            if section_text.lower() in clean(tag.get_text()).lower():
                heading = tag
                break
        if not heading:
            continue
        items: list = []
        el = heading.find_next_sibling()
        while el and el.name not in ("h3", "h4", "h5"):
            for li in el.find_all("li") if el.name in ("ul", "ol") else [el]:
                t = clean(li.get_text())
                if t and t.lower() not in (section_text.lower(), ""):
                    items.append(t)
            el = el.find_next_sibling()
        result[key] = items
    return result


# ---------------------------------------------------------------------------
# Tab content extraction
# ---------------------------------------------------------------------------

# Maps known E4C field labels to (tab_key, field_key)
# Covers all five tabs across all technology categories.
FIELD_MAP: dict = {
    # -- snapshot --
    "Market Suggested Retail Price":              ("snapshot", "price"),
    "Target Users (Target Impact Group)":         ("snapshot", "target_users"),
    "Distributors / Implementing Organizations":  ("snapshot", "distributors"),
    "Competitive Landscape":                      ("snapshot", "competitive_landscape"),
    "Regions":                                    ("snapshot", "regions"),
    "Manufacturing/Building Method":              ("snapshot", "manufacturing_method"),
    "Intellectual Property Type":                 ("snapshot", "ip_type"),
    "Intellectural Property Type":                ("snapshot", "ip_type"),    # E4C typo
    "User Provision Model":                       ("snapshot", "user_provision_model"),
    "Distributions to Date Status":               ("snapshot", "distributions_to_date"),
    "Distributions to Date":                      ("snapshot", "distributions_to_date"),
    "Target SDGs":                                ("snapshot", "sdgs_raw"),

    # -- manufacturing_delivery --
    "Description of the combined methods":             ("manufacturing_delivery", "treatment_methods"),
    "Manufacturing/Building Method":                   ("manufacturing_delivery", "manufacturing_method"),
    "Local Production Feasibility":                    ("manufacturing_delivery", "local_production_feasibility"),
    "Supply Chain Description":                        ("manufacturing_delivery", "supply_chain"),
    "Production Capacity":                             ("manufacturing_delivery", "production_capacity"),

    # -- performance_use: water --
    "Manufacturer-specified water treatment rate (L/hr)": ("performance_use", "water_treatment_rate_l_hr"),
    "Bacteria reduction":                              ("performance_use", "bacteria_reduction"),
    "Virus reduction":                                 ("performance_use", "virus_reduction"),
    "Protozoa reduction":                              ("performance_use", "protozoa_reduction"),
    "Heavy metals and/or arsenic reduction":           ("performance_use", "heavy_metals_arsenic_reduction"),
    "Maximum recommended influent turbidity level (NTU)": ("performance_use", "influent_turbidity_ntu"),
    "Effluent turbidity levels (NTU)":                 ("performance_use", "effluent_turbidity_ntu"),
    "Safe water storage capacity (L)":                 ("performance_use", "safe_storage_capacity_l"),
    "Manufacturer-specified lifetime volume (L)":      ("performance_use", "lifetime_volume_l"),

    # -- performance_use: energy --
    "Power output (W)":                                ("performance_use", "power_output_w"),
    "Panel efficiency (%)":                            ("performance_use", "panel_efficiency_pct"),
    "Battery capacity (Wh)":                           ("performance_use", "battery_capacity_wh"),
    "Battery cycle life":                              ("performance_use", "battery_cycle_life"),
    "Lumen output":                                    ("performance_use", "lumen_output_lm"),
    "Run time":                                        ("performance_use", "run_time_hrs"),
    "Thermal efficiency":                              ("performance_use", "thermal_efficiency_pct"),
    "PM2.5 emissions":                                 ("performance_use", "pm25_emissions"),
    "CO emissions":                                    ("performance_use", "co_emissions"),

    # -- performance_use: general --
    "Consumables":                                     ("performance_use", "consumables"),
    "Design Specifications":                           ("performance_use", "design_specifications"),
    "Technical Support":                               ("performance_use", "technical_support"),
    "Replacement Components":                          ("performance_use", "replacement_components"),
    "Lifecycle":                                       ("performance_use", "lifecycle"),
    "Manufacturer Specified Performance Parameters":   ("performance_use", "manufacturer_performance_params"),
    "Vetted Performance Status":                       ("performance_use", "vetted_performance_status"),
    "Safety":                                          ("performance_use", "safety_notes"),
    "Complementary Technical Systems":                 ("performance_use", "complementary_systems"),
    "Product Schematics":                              ("performance_use", "schematics_note"),

    # -- performance_use: ICT/health --
    "Operating system and version":                    ("performance_use", "operating_system"),
    "Languages available (list)":                      ("performance_use", "languages"),
    "Power requirements":                              ("performance_use", "power_requirements"),
    "Sensitivity":                                     ("performance_use", "sensitivity_pct"),
    "Specificity":                                     ("performance_use", "specificity_pct"),
    "Temperature range":                               ("performance_use", "temperature_range"),
    "Weight capacity":                                 ("performance_use", "weight_capacity_kg"),
    "Load capacity":                                   ("performance_use", "load_capacity_kg"),
    "IP rating":                                       ("performance_use", "ip_rating"),

    # -- research_standards --
    "Compliance with regulations":                     ("research_standards", "regulatory_compliance"),
    "Evaluation methods":                              ("research_standards", "evaluation_methods"),
    "Academic Research and References":                ("research_standards", "academic_references"),
    "Other Information":                               ("research_standards", "other_information"),

    # -- feedback --
    "Feedback":                                        ("feedback", "feedback_summary"),
}


def _get_sibling_text(el, max_siblings: int = 4) -> str:
    """Collect text from the next few siblings after a label element."""
    parts: list = []
    sib = el.next_sibling
    count = 0
    while sib and count < max_siblings:
        if isinstance(sib, NavigableString):
            t = sib.strip()
            if t:
                parts.append(t)
        elif isinstance(sib, Tag):
            if sib.name in ("h2", "h3", "h4", "h5"):
                break
            t = clean(sib.get_text(" "))
            if t:
                parts.append(t)
            count += 1
        sib = sib.next_sibling
    return clean(" ".join(parts))


def extract_known_fields(soup: BeautifulSoup) -> dict:
    """
    Primary extractor: find every known field label in FIELD_MAP and
    collect the content that follows it. Returns tab-structured dict.
    """
    tabs: dict = {
        "snapshot": {},
        "manufacturing_delivery": {},
        "performance_use": {},
        "research_standards": {},
        "feedback": {},
    }

    for label, (tab, key) in FIELD_MAP.items():
        if key in tabs[tab]:  # already filled by an earlier match
            continue

        # Strategy A: find tag whose text exactly equals label
        found = None
        for tag in soup.find_all(["h3", "h4", "h5", "dt", "strong", "b", "td", "th"]):
            if clean(tag.get_text()) == label:
                found = tag
                break

        if found:
            if found.name == "dt":
                dd = found.find_next_sibling("dd")
                value = clean(dd.get_text(" ")) if dd else ""
            elif found.name in ("td", "th"):
                next_td = found.find_next_sibling("td")
                value = clean(next_td.get_text(" ")) if next_td else ""
            else:
                value = _get_sibling_text(found)
            if value:
                tabs[tab][key] = value
            continue

        # Strategy B: regex on full page text for "Label\nValue" patterns
        page_text = soup.get_text("\n")
        pattern = re.compile(
            r"(?:^|\n)" + re.escape(label) + r"\s*\n\s*(.+?)(?:\n[A-Z\u2013\u2014]|\Z)",
            re.MULTILINE,
        )
        m = pattern.search(page_text)
        if m:
            value = clean(m.group(1))
            if value:
                tabs[tab][key] = value[:600]

    return tabs


def extract_all_h3_sections(soup: BeautifulSoup) -> dict:
    """
    Secondary extractor: walk every h3 heading, collect following paragraphs,
    and store as free-text under a best-guess tab.
    This catches any field not covered by FIELD_MAP.
    """
    # Rough mapping of heading keywords to tabs
    TAB_KEYWORDS: dict = {
        "snapshot": [
            "product description", "target sdg", "market suggested",
            "target user", "distributor", "competitive", "region",
            "manufacturing/building", "intellectual property",
            "user provision", "distributions to date",
        ],
        "manufacturing_delivery": [
            "description of the combined", "manufacturing", "delivery",
            "supply chain", "production capacity", "local production",
        ],
        "performance_use": [
            "performance", "design specification", "technical support",
            "replacement", "lifecycle", "vetted", "safety",
            "complementary", "consumable", "schematics",
            "bacteria", "virus", "protozoa", "turbidity",
            "power output", "efficiency", "lumen", "thermal",
            "operating system", "languages available",
        ],
        "research_standards": [
            "academic research", "compliance", "evaluation method",
            "other information", "research", "standard",
        ],
        "feedback": ["feedback", "user feedback", "field report"],
    }

    def guess_tab(heading_text: str) -> str:
        lower = heading_text.lower()
        for tab, keywords in TAB_KEYWORDS.items():
            if any(kw in lower for kw in keywords):
                return tab
        return "snapshot"

    sections: dict = {t: {} for t in TAB_KEYWORDS}

    main = (
        soup.find("div", class_=re.compile(
            r"product[-_]content|solution[-_]content|entry[-_]content|post[-_]content",
            re.I
        ))
        or soup.find("main")
        or soup.find("article")
        or soup.body
    )
    if not main:
        return sections

    for h3 in main.find_all("h3"):
        heading = clean(h3.get_text())
        if not heading:
            continue
        tab = guess_tab(heading)
        if heading in sections[tab]:
            continue

        parts: list = []
        el = h3.find_next_sibling()
        while el and el.name not in ("h2", "h3"):
            t = clean(el.get_text(" "))
            if t:
                parts.append(t)
            el = el.find_next_sibling()

        if parts:
            sections[tab][heading] = clean(" | ".join(parts[:5]))

    return sections


# ---------------------------------------------------------------------------
# Product page scraper
# ---------------------------------------------------------------------------

def scrape_product(url: str) -> Optional[dict]:
    soup = fetch(url)
    if not soup:
        return None

    slug = slug_from_url(url)

    # Name
    h1 = soup.find("h1")
    name = clean(h1.get_text()) if h1 else slug

    # Short description: first substantial paragraph after h1
    description = ""
    if h1:
        for sib in h1.find_next_siblings():
            if sib.name == "p":
                t = clean(sib.get_text())
                if len(t) > 30:
                    description = t
                    break

    # Dates
    page_text = soup.get_text(" ")
    updated_m = re.search(r"Updated on\s+([A-Za-z]+ \d{1,2},?\s*\d{4})", page_text)
    created_m = re.search(r"Created on\s+([A-Za-z]+ \d{1,2},?\s*\d{4})", page_text)

    # SDGs from visible text
    sdgs = list(dict.fromkeys(
        clean(m.group()) for m in
        re.finditer(r"SDG \d+[:\s][^\n\.]{5,60}", page_text)
    ))

    # Taxonomy from breadcrumb
    taxonomy = extract_breadcrumb(soup)

    # Attribution
    attribution = extract_attribution(soup)

    # Tab content -- primary FIELD_MAP pass
    tabs = extract_known_fields(soup)

    # Tab content -- secondary free-text h3 pass
    h3_sections = extract_all_h3_sections(soup)
    for tab_key, fields in h3_sections.items():
        for field_key, value in fields.items():
            # Only add if not already populated by primary pass
            if field_key not in tabs[tab_key]:
                tabs[tab_key][field_key] = value

    # Similar solutions
    similar = sorted(set(
        urljoin(BASE_URL, a["href"]).split("?")[0].rstrip("/") + "/"
        for a in soup.find_all("a", href=True)
        if "/solutions/product/" in a["href"]
        and slug not in a["href"]
    ))[:10]

    return {
        "slug":                   slug,
        "url":                    url,
        "name":                   name,
        "description":            description,
        "updated_on":             updated_m.group(1) if updated_m else None,
        "created_on":             created_m.group(1) if created_m else None,
        "sdgs":                   sdgs,
        "taxonomy":               taxonomy,
        "attribution":            attribution,
        "snapshot":               tabs["snapshot"],
        "manufacturing_delivery": tabs["manufacturing_delivery"],
        "performance_use":        tabs["performance_use"],
        "research_standards":     tabs["research_standards"],
        "feedback":               tabs["feedback"],
        "similar_solutions":      similar,
        "scraped_at":             time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

def scrape_and_save(url: str, out_dir: Path) -> tuple:
    slug = slug_from_url(url)
    out_path = out_dir / f"{slug}.json"

    if out_path.exists():
        log.debug(f"Skip (already done): {slug}")
        return url, True

    time.sleep(DELAY)

    data = scrape_product(url)
    if data is None:
        return url, False

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return url, True


# ---------------------------------------------------------------------------
# Merge & ES export
# ---------------------------------------------------------------------------

def merge_all(out_dir: Path, merged_path: Path) -> int:
    all_data: list = []
    for jf in sorted(out_dir.glob("*.json")):
        with open(jf, encoding="utf-8") as f:
            try:
                all_data.append(json.load(f))
            except json.JSONDecodeError as exc:
                log.warning(f"Skipping corrupt file {jf}: {exc}")
    with open(merged_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    return len(all_data)


def build_es_bulk(merged_path: Path, out_path: Path, index: str = "e4c_benchmarks"):
    """
    Generate Elasticsearch bulk import NDJSON from merged dataset.

    Import command:
        curl -X POST http://localhost:9200/_bulk \\
          -H 'Content-Type: application/x-ndjson' \\
          --data-binary @e4c_es_bulk.ndjson
    """
    with open(merged_path, encoding="utf-8") as f:
        solutions = json.load(f)

    lines: list = []
    for sol in solutions:
        snap = sol.get("snapshot", {})
        perf = sol.get("performance_use", {})
        mfg  = sol.get("manufacturing_delivery", {})
        res  = sol.get("research_standards", {})
        tax  = sol.get("taxonomy", {})
        attr = sol.get("attribution", {})

        meta = {"index": {"_index": index, "_id": sol.get("slug")}}

        doc = {
            # Identity
            "slug":              sol.get("slug"),
            "name":              sol.get("name"),
            "description":       sol.get("description"),
            "url":               sol.get("url"),
            "updated_on":        sol.get("updated_on"),
            "created_on":        sol.get("created_on"),
            "scraped_at":        sol.get("scraped_at"),

            # Taxonomy (keyword fields for filtering)
            "sector":            tax.get("sector"),
            "sub_sector":        tax.get("sub_sector"),
            "category":          tax.get("category"),
            "sub_category":      tax.get("sub_category"),

            # Attribution
            "developed_by":      attr.get("developed_by", []),
            "tested_by":         attr.get("tested_by", []),

            # Snapshot fields (keyword/text for search)
            "sdgs":              sol.get("sdgs", []),
            "regions":           snap.get("regions"),
            "price_raw":         snap.get("price"),
            "ip_type":           snap.get("ip_type"),
            "distributions_to_date": snap.get("distributions_to_date"),
            "target_users":      snap.get("target_users"),
            "distributors":      snap.get("distributors"),

            # Manufacturing
            "treatment_methods":         mfg.get("treatment_methods"),
            "local_production_feasibility": mfg.get("local_production_feasibility"),
            "supply_chain":              mfg.get("supply_chain"),

            # Performance -- water
            "bacteria_reduction":        perf.get("bacteria_reduction"),
            "virus_reduction":           perf.get("virus_reduction"),
            "protozoa_reduction":        perf.get("protozoa_reduction"),
            "heavy_metals_reduction":    perf.get("heavy_metals_arsenic_reduction"),
            "effluent_turbidity_ntu":    perf.get("effluent_turbidity_ntu"),
            "influent_turbidity_ntu":    perf.get("influent_turbidity_ntu"),
            "water_treatment_rate_l_hr": perf.get("water_treatment_rate_l_hr"),
            "lifetime_volume_l":         perf.get("lifetime_volume_l"),

            # Performance -- energy
            "power_output_w":            perf.get("power_output_w"),
            "panel_efficiency_pct":      perf.get("panel_efficiency_pct"),
            "battery_capacity_wh":       perf.get("battery_capacity_wh"),
            "battery_cycle_life":        perf.get("battery_cycle_life"),
            "lumen_output_lm":           perf.get("lumen_output_lm"),
            "thermal_efficiency_pct":    perf.get("thermal_efficiency_pct"),
            "pm25_emissions":            perf.get("pm25_emissions"),
            "co_emissions":              perf.get("co_emissions"),

            # Performance -- health/ICT
            "sensitivity_pct":           perf.get("sensitivity_pct"),
            "specificity_pct":           perf.get("specificity_pct"),
            "operating_system":          perf.get("operating_system"),
            "languages":                 perf.get("languages"),
            "ip_rating":                 perf.get("ip_rating"),

            # Performance -- general
            "lifecycle":                 perf.get("lifecycle"),
            "vetted_performance_status": perf.get("vetted_performance_status"),
            "consumables":               perf.get("consumables"),
            "replacement_components":    perf.get("replacement_components"),

            # Standards
            "regulatory_compliance":     res.get("regulatory_compliance"),
            "evaluation_methods":        res.get("evaluation_methods"),

            # Full tab blobs for rich full-text search
            "snapshot_full":             snap,
            "manufacturing_full":        mfg,
            "performance_full":          perf,
            "standards_full":            res,
            "feedback_full":             sol.get("feedback", {}),
        }

        lines.append(json.dumps(meta, ensure_ascii=False))
        lines.append(json.dumps(doc, ensure_ascii=False))

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    log.info(f"ES bulk payload: {len(solutions)} docs -> {out_path}")
    print(f"\n  Import with:")
    print(f"  curl -X POST http://localhost:9200/_bulk \\")
    print(f"    -H 'Content-Type: application/x-ndjson' \\")
    print(f"    --data-binary @{out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Step 1 -- Discover links (cached)
    if LINKS_CACHE.exists():
        with open(LINKS_CACHE) as f:
            all_links = json.load(f)
        log.info(f"Loaded {len(all_links)} cached links from {LINKS_CACHE}")
    else:
        all_links = discover_product_links()
        with open(LINKS_CACHE, "w") as f:
            json.dump(all_links, f, indent=2)
        log.info(f"Saved {len(all_links)} links to {LINKS_CACHE}")

    if not all_links:
        log.error("No product links found. Aborting.")
        return

    # Step 2 -- Scrape
    log.info(f"Scraping {len(all_links)} products ({MAX_WORKERS} threads)...")
    errors: list = []
    done = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(scrape_and_save, url, OUTPUT_DIR): url
            for url in all_links
        }
        for future in as_completed(futures):
            url, ok = future.result()
            done += 1
            if not ok:
                errors.append(url)
                log.warning(f"FAILED ({done}/{len(all_links)}): {url}")
            elif done % 50 == 0:
                log.info(f"Progress: {done}/{len(all_links)} | errors so far: {len(errors)}")

    log.info(f"Done. {done - len(errors)} ok, {len(errors)} failed.")

    if errors:
        with open(ERRORS_OUTPUT, "w") as f:
            json.dump(errors, f, indent=2)
        log.info(f"Error list -> {ERRORS_OUTPUT}")

    # Step 3 -- Merge
    log.info(f"Merging -> {MERGED_OUTPUT}")
    count = merge_all(OUTPUT_DIR, MERGED_OUTPUT)
    print(f"\n  {count} solutions -> {MERGED_OUTPUT}")
    if errors:
        print(f"  {len(errors)} failed -> {ERRORS_OUTPUT}  (run with 'retry' to retry)")


def retry_errors():
    if not ERRORS_OUTPUT.exists():
        print("No error file found.")
        return
    with open(ERRORS_OUTPUT) as f:
        errors = json.load(f)
    if not errors:
        print("Error file is empty -- nothing to retry.")
        return
    log.info(f"Retrying {len(errors)} failed URLs...")
    still_failing: list = []
    for url in errors:
        _, ok = scrape_and_save(url, OUTPUT_DIR)
        if not ok:
            still_failing.append(url)
    with open(ERRORS_OUTPUT, "w") as f:
        json.dump(still_failing, f, indent=2)
    print(f"  {len(errors) - len(still_failing)} recovered, {len(still_failing)} still failing.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    cmds = {
        "retry":    retry_errors,
        "merge":    lambda: print(f"  {merge_all(OUTPUT_DIR, MERGED_OUTPUT)} solutions merged -> {MERGED_OUTPUT}"),
        "build-es": lambda: build_es_bulk(MERGED_OUTPUT, Path("e4c_es_bulk.ndjson")),
    }

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd in cmds:
            cmds[cmd]()
        else:
            print(f"Unknown command: {cmd}")
            print(f"Usage: python e4c_scraper.py [{' | '.join(cmds)}]")
    else:
        main()
