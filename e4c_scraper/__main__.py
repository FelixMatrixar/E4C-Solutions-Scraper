import json
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from .config import (
    log, OUTPUT_DIR, MERGED_OUTPUT, ERRORS_OUTPUT, LINKS_CACHE, MAX_WORKERS
)
from .client import discover_product_links
from .storage import scrape_and_save, merge_all, build_es_bulk

def run_scraper():
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Step 1 -- Discover links
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

def main():
    parser = argparse.ArgumentParser(description="E4C Solutions Library Scraper")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Run command (default)
    subparsers.add_parser("run", help="Run the full scraper")
    
    # Retry command
    subparsers.add_parser("retry", help="Retry failed URLs only")
    
    # Merge command
    subparsers.add_parser("merge", help="Re-merge individual files without re-scraping")
    
    # Build-es command
    subparsers.add_parser("build-es", help="Build Elasticsearch bulk import NDJSON")

    args = parser.parse_args()

    if args.command == "retry":
        retry_errors()
    elif args.command == "merge":
        count = merge_all(OUTPUT_DIR, MERGED_OUTPUT)
        print(f"  {count} solutions merged -> {MERGED_OUTPUT}")
    elif args.command == "build-es":
        build_es_bulk(MERGED_OUTPUT, Path("e4c_es_bulk.ndjson"))
    elif args.command == "run" or args.command is None:
        run_scraper()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
