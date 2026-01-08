import re
import time
from bs4 import BeautifulSoup, NavigableString, Tag
from urllib.parse import urljoin, urlparse
from typing import Optional, Dict
from .config import BASE_URL, FIELD_MAP
from .client import fetch
from .models import (
    Product, Taxonomy, Attribution, Snapshot,
    ManufacturingDelivery, PerformanceUse, ResearchStandards, Feedback
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())

def slug_from_url(url: str) -> str:
    return urlparse(url).path.rstrip("/").split("/")[-1]

def extract_breadcrumb(soup: BeautifulSoup) -> Taxonomy:
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
    return Taxonomy(**result)

def extract_attribution(soup: BeautifulSoup) -> Attribution:
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
    return Attribution(**result)

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

def extract_known_fields(soup: BeautifulSoup) -> Dict[str, dict]:
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

def extract_all_h3_sections(soup: BeautifulSoup) -> Dict[str, dict]:
    """
    Secondary extractor: walk every h3 heading, collect following paragraphs,
    and store as free-text under a best-guess tab.
    """
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

def scrape_product(url: str) -> Optional[Product]:
    soup = fetch(url)
    if not soup:
        return None

    slug = slug_from_url(url)
    h1 = soup.find("h1")
    name = clean(h1.get_text()) if h1 else slug

    description = ""
    if h1:
        for sib in h1.find_next_siblings():
            if sib.name == "p":
                t = clean(sib.get_text())
                if len(t) > 30:
                    description = t
                    break

    page_text = soup.get_text(" ")
    updated_m = re.search(r"Updated on\s+([A-Za-z]+ \d{1,2},?\s*\d{4})", page_text)
    created_m = re.search(r"Created on\s+([A-Za-z]+ \d{1,2},?\s*\d{4})", page_text)

    sdgs = list(dict.fromkeys(
        clean(m.group()) for m in
        re.finditer(r"SDG \d+[:\s][^\n\.]{5,60}", page_text)
    ))

    taxonomy = extract_breadcrumb(soup)
    attribution = extract_attribution(soup)
    tabs_raw = extract_known_fields(soup)
    h3_sections = extract_all_h3_sections(soup)

    # Merge h3_sections into tabs_raw and then into models
    processed_tabs = {}
    model_map = {
        "snapshot": Snapshot,
        "manufacturing_delivery": ManufacturingDelivery,
        "performance_use": PerformanceUse,
        "research_standards": ResearchStandards,
        "feedback": Feedback
    }

    for tab_key, model_class in model_map.items():
        # Combine base fields and h3 sections
        combined_data = tabs_raw.get(tab_key, {}).copy()
        
        # Add h3 sections, preserving existing values
        for field_key, value in h3_sections.get(tab_key, {}).items():
            if field_key not in combined_data:
                combined_data[field_key] = value
        
        processed_tabs[tab_key] = model_class(**combined_data)

    similar = sorted(set(
        urljoin(BASE_URL, a["href"]).split("?")[0].rstrip("/") + "/"
        for a in soup.find_all("a", href=True)
        if "/solutions/product/" in a["href"]
        and slug not in a["href"]
    ))[:10]

    return Product(
        slug=slug,
        url=url,
        name=name,
        description=description,
        updated_on=updated_m.group(1) if updated_m else None,
        created_on=created_m.group(1) if created_m else None,
        sdgs=sdgs,
        taxonomy=taxonomy,
        attribution=attribution,
        snapshot=processed_tabs["snapshot"],
        manufacturing_delivery=processed_tabs["manufacturing_delivery"],
        performance_use=processed_tabs["performance_use"],
        research_standards=processed_tabs["research_standards"],
        feedback=processed_tabs["feedback"],
        similar_solutions=similar
    )
