from pathlib import Path
import random
from typing import Any

import pandas as pd

from app.data.warehouse import ensure_data_dirs

STAGES = ["awareness", "consideration", "purchase", "satisfaction", "loyalty"]
BRANDS = ["Atlas", "Beacon", "Crest", "Drift", "Ember", "Flux"]
BASE_RATES = {
    "awareness": 0.65,
    "consideration": 0.5,
    "purchase": 0.35,
    "satisfaction": 0.45,
    "loyalty": 0.3,
}
BRAND_MULTIPLIER = {
    "Atlas": 1.05,
    "Beacon": 0.95,
    "Crest": 1.1,
    "Drift": 0.9,
    "Ember": 1.0,
    "Flux": 0.85,
}


def _clamp(value: float) -> float:
    return max(0.02, min(0.98, value))


def generate_demo_data(respondents: int = 500) -> tuple[Path, dict[str, Any]]:
    dirs = ensure_data_dirs()
    rows: list[dict[str, Any]] = []
    study_id = "demo_001"

    for respondent_id in range(1, respondents + 1):
        for stage in STAGES:
            for brand in BRANDS:
                rate = _clamp(BASE_RATES[stage] * BRAND_MULTIPLIER[brand])
                value = 1 if random.random() < rate else 0
                rows.append(
                    {
                        "study_id": study_id,
                        "respondent_id": respondent_id,
                        "stage": stage,
                        "brand": brand,
                        "value": value,
                    }
                )

    df = pd.DataFrame(rows)
    parquet_path = dirs["curated"] / "fact_journey_demo.parquet"
    df.to_parquet(parquet_path, index=False)

    stats = {
        "rows": len(df),
        "respondents": respondents,
        "brands": len(BRANDS),
        "stages": len(STAGES),
    }
    return parquet_path, stats


if __name__ == "__main__":
    path, stats = generate_demo_data()
    print(f"Seeded demo data at {path}")
    print(stats)
