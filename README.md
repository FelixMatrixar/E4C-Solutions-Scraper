# E4C Solutions Scraper

A modular, model-driven Python scraper for the [Engineering for Change (E4C) Solutions Library](https://www.engineeringforchange.org/solutions-library/).

## Features
- **Modular Design**: Separated concerns for configuration, networking, parsing, and storage.
- **Model-Driven**: Uses **Pydantic v2** for robust data validation and structured output.
- **Dual-Stage Discovery**: Discovers product URLs via WordPress sitemaps and BFS crawling of internal links.
- **Resume-Safe**: Skips already-scraped products and provides a retry mechanism for failed URLs.
- **Elasticsearch Ready**: Generates NDJSON bulk import payloads for Elasticsearch.

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd "E4C Solutions Scraper"
   ```

2. **Create and activate a virtual environment**:
   ```bash
   python -m venv venv
   # On Windows:
   .\venv\Scripts\activate
   # On macOS/Linux:
   source venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install requests beautifulsoup4 lxml pydantic
   ```

## Usage

The scraper is implemented as a Python package. You can run it using `python -m e4c_scraper` followed by a command.

### Commands

- **Run the full scraper**:
  ```bash
  python -m e4c_scraper run
  ```
  Scrapes all discovered products and saves them as individual JSON files in `e4c_solutions/`.

- **Retry failed URLs**:
  ```bash
  python -m e4c_scraper retry
  ```
  Retries only the URLs listed in `e4c_scrape_errors.json`.

- **Merge results**:
  ```bash
  python -m e4c_scraper merge
  ```
  Merges all individual JSON files into a single `e4c_solutions_all.json` dataset.

- **Build Elasticsearch bulk import**:
  ```bash
  python -m e4c_scraper build-es
  ```
  Generates `e4c_es_bulk.ndjson` for bulk importing into Elasticsearch.

## Project Structure

- `e4c_scraper/`
  - `config.py`: Configuration constants, URLs, and extraction mappings.
  - `models.py`: Pydantic models for structured product data.
  - `client.py`: Network requests and link discovery logic.
  - `parser.py`: HTML parsing and data extraction.
  - `storage.py`: File I/O, merging, and ES export logic.
  - `__main__.py`: CLI entry point.
- `e4c_scraper.py`: Legacy wrapper script.
- `e4c_solutions/`: Directory containing individual product JSON files.

## Output Files

- `e4c_solutions/`: Individual product data (slug-based filenames).
- `e4c_solutions_all.json`: The full merged dataset.
- `e4c_product_links.json`: Cached list of discovered product URLs.
- `e4c_scrape_errors.json`: List of URLs that failed during the last run.
- `e4c_scrape.log`: Execution logs.
