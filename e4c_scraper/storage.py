import json
import time
from pathlib import Path
from .config import log, DELAY
from .parser import slug_from_url, scrape_product

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
        # Use model_dump_json for Pydantic v2
        f.write(data.model_dump_json(indent=2))

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
    """
    if not merged_path.exists():
        log.error(f"Merged file {merged_path} not found.")
        return

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

            # Taxonomy
            "sector":            tax.get("sector"),
            "sub_sector":        tax.get("sub_sector"),
            "category":          tax.get("category"),
            "sub_category":      tax.get("sub_category"),

            # Attribution
            "developed_by":      attr.get("developed_by", []),
            "tested_by":         attr.get("tested_by", []),

            # Snapshot fields
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

            # Full tab blobs
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
