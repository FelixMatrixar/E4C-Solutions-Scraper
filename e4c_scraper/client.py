import requests
import time
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import Optional
from .config import log, HEADERS, MAX_RETRIES, RETRY_DELAY, BASE_URL, DELAY

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
    seeds = list(links) if links else []
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
    MAX_BFS_FETCHES = 200

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
