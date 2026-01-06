import logging
from pathlib import Path

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
log = logging.getLogger("e4c_scraper")

# ---------------------------------------------------------------------------
# Tab content extraction mapping
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
