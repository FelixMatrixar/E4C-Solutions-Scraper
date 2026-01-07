from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Dict
from datetime import datetime

class BaseE4CModel(BaseModel):
    model_config = ConfigDict(extra='allow')

class Taxonomy(BaseE4CModel):
    sector: Optional[str] = None
    sub_sector: Optional[str] = None
    category: Optional[str] = None
    sub_category: Optional[str] = None

class Attribution(BaseE4CModel):
    developed_by: List[str] = Field(default_factory=list)
    tested_by: List[str] = Field(default_factory=list)
    content_partners: List[str] = Field(default_factory=list)

class Snapshot(BaseE4CModel):
    price: Optional[str] = None
    target_users: Optional[str] = None
    distributors: Optional[str] = None
    competitive_landscape: Optional[str] = None
    regions: Optional[str] = None
    manufacturing_method: Optional[str] = None
    ip_type: Optional[str] = None
    user_provision_model: Optional[str] = None
    distributions_to_date: Optional[str] = None
    sdgs_raw: Optional[str] = None

class ManufacturingDelivery(BaseE4CModel):
    treatment_methods: Optional[str] = None
    manufacturing_method: Optional[str] = None
    local_production_feasibility: Optional[str] = None
    supply_chain: Optional[str] = None
    production_capacity: Optional[str] = None

class PerformanceUse(BaseE4CModel):
    # Water
    water_treatment_rate_l_hr: Optional[str] = None
    bacteria_reduction: Optional[str] = None
    virus_reduction: Optional[str] = None
    protozoa_reduction: Optional[str] = None
    heavy_metals_arsenic_reduction: Optional[str] = None
    influent_turbidity_ntu: Optional[str] = None
    effluent_turbidity_ntu: Optional[str] = None
    safe_storage_capacity_l: Optional[str] = None
    lifetime_volume_l: Optional[str] = None

    # Energy
    power_output_w: Optional[str] = None
    panel_efficiency_pct: Optional[str] = None
    battery_capacity_wh: Optional[str] = None
    battery_cycle_life: Optional[str] = None
    lumen_output_lm: Optional[str] = None
    run_time_hrs: Optional[str] = None
    thermal_efficiency_pct: Optional[str] = None
    pm25_emissions: Optional[str] = None
    co_emissions: Optional[str] = None

    # ICT/Health
    operating_system: Optional[str] = None
    languages: Optional[str] = None
    power_requirements: Optional[str] = None
    sensitivity_pct: Optional[str] = None
    specificity_pct: Optional[str] = None
    temperature_range: Optional[str] = None
    weight_capacity_kg: Optional[str] = None
    load_capacity_kg: Optional[str] = None
    ip_rating: Optional[str] = None

    # General
    consumables: Optional[str] = None
    design_specifications: Optional[str] = None
    technical_support: Optional[str] = None
    replacement_components: Optional[str] = None
    lifecycle: Optional[str] = None
    manufacturer_performance_params: Optional[str] = None
    vetted_performance_status: Optional[str] = None
    safety_notes: Optional[str] = None
    complementary_systems: Optional[str] = None
    schematics_note: Optional[str] = None

class ResearchStandards(BaseE4CModel):
    regulatory_compliance: Optional[str] = None
    evaluation_methods: Optional[str] = None
    academic_references: Optional[str] = None
    other_information: Optional[str] = None

class Feedback(BaseE4CModel):
    feedback_summary: Optional[str] = None

class Product(BaseE4CModel):
    slug: str
    url: str
    name: str
    description: Optional[str] = None
    updated_on: Optional[str] = None
    created_on: Optional[str] = None
    sdgs: List[str] = Field(default_factory=list)
    taxonomy: Taxonomy = Field(default_factory=Taxonomy)
    attribution: Attribution = Field(default_factory=Attribution)
    snapshot: Snapshot = Field(default_factory=Snapshot)
    manufacturing_delivery: ManufacturingDelivery = Field(default_factory=ManufacturingDelivery)
    performance_use: PerformanceUse = Field(default_factory=PerformanceUse)
    research_standards: ResearchStandards = Field(default_factory=ResearchStandards)
    feedback: Feedback = Field(default_factory=Feedback)
    similar_solutions: List[str] = Field(default_factory=list)
    scraped_at: str = Field(default_factory=lambda: datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
