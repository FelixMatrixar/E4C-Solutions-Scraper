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

def discover_product_links() -> dict:
    """
    Multi-stage URL discovery bypassing JS traps.
    Returns a dictionary to maintain compatibility with the scraper pipeline.
    """
    discovered_products: dict = {}

    # ── Stage 1: The WordPress REST API (Fastest) ─────────────────────────────
    log.info("Stage 1: Attempting to pull from WordPress REST API...")
    api_success = False
    
    for endpoint in ["solution", "solutions", "product"]:
        if api_success: break
        
        for page in range(1, 25):  # 25 pages * 100 items = up to 2500 links
            api_url = f"{BASE_URL}/wp-json/wp/v2/{endpoint}?per_page=100&page={page}"
            try:
                r = session.get(api_url, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    if not data: 
                        break  # End of pagination
                    
                    added = 0
                    for item in data:
                        if "link" in item:
                            url = item["link"].rstrip("/") + "/"
                            if url not in discovered_products:
                                # We grab basic metadata from the API if possible
                                title = item.get("title", {}).get("rendered", "Unknown")
                                discovered_products[url] = {
                                    "url": url,
                                    "name": title,
                                    "date_added": item.get("date", "").split("T")[0],
                                    "sector": None, # Will extract during deep scrape
                                    "company": None, # Will extract during deep scrape
                                    "tags": []
                                }
                                added += 1
                    
                    log.info(f"  API ({endpoint}) page {page}: +{added} URLs")
                    api_success = True
                else:
                    break 
            except Exception:
                break

    # ── Stage 2: Deep BFS Spidering (The Fallback) ────────────────────────────
    log.info(f"Stage 2: Deep BFS crawl (starting with {len(discovered_products)} known links)...")
    
    queue = list(discovered_products.keys()) + [
        f"{BASE_URL}/solutions/product/splash-stations/",
        f"{BASE_URL}/solutions/product/lifestraw-family-1-0/",
        f"{BASE_URL}/solutions/product/jikojoy-charcoal-stove/",
        f"{BASE_URL}/solutions/product/kio-kit/",
    ]
    queue = list(dict.fromkeys(queue)) # Remove duplicates
    
    visited: set = set()
    bfs_fetches = 0
    MAX_BFS_FETCHES = 1500

    while queue and bfs_fetches < MAX_BFS_FETCHES:
        url = queue.pop(0)
        normalized = url.rstrip("/") + "/"
        
        if normalized in visited:
            continue
            
        visited.add(normalized)
        bfs_fetches += 1

        # If it's a completely new URL from the BFS, add it to our dictionary
        if normalized not in discovered_products:
            discovered_products[normalized] = {
                "url": normalized,
                "name": None,
                "date_added": None,
                "sector": None,
                "company": None,
                "tags": []
            }

        soup = fetch(url)
        if not soup:
            continue

        found_on_page = 0
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/solutions/product/" in href:
                full_url = urljoin(BASE_URL, href).rstrip("/") + "/"
                if full_url not in visited and full_url not in queue:
                    queue.append(full_url)
                    found_on_page += 1

        if bfs_fetches % 10 == 0 or len(queue) == 0:
            log.info(f"  BFS [{bfs_fetches}] -> queue={len(queue)} | total_discovered={len(discovered_products)}")
        
        time.sleep(DELAY)

    log.info(f"Discovery complete: {len(discovered_products)} unique product URLs.")
    return discovered_products