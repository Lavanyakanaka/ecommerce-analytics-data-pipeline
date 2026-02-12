import json
import logging
from datetime import datetime
from pathlib import Path

import yaml

from scripts.data_generation import generate_data
from scripts.ingestion import ingest_to_staging
from scripts.quality_checks import validate_data
from scripts.transformation import generate_analytics, load_warehouse, staging_to_production


OUT = Path("data/processed")
OUT.mkdir(parents=True, exist_ok=True)


def _load_pipeline_config() -> dict:
    config_path = Path("config/config.yaml")
    if config_path.exists():
        with config_path.open("r", encoding="utf-8") as f:
            return (yaml.safe_load(f) or {}).get("pipeline", {})
    return {}


def _run_with_retries(step_name: str, func, retries: int) -> dict:
    last_error = None
    for attempt in range(1, retries + 2):
        try:
            logging.info("Starting step %s (attempt %s)", step_name, attempt)
            result = func()
            logging.info("Completed step %s", step_name)
            return {"step": step_name, "status": "success", "attempt": attempt, "result": result}
        except Exception as exc:
            last_error = str(exc)
            logging.warning("Step %s failed on attempt %s: %s", step_name, attempt, last_error)
    return {"step": step_name, "status": "failed", "error": last_error}


def run_pipeline() -> dict:
    config = _load_pipeline_config()
    retries = int(config.get("retries", 0))
    logging.basicConfig(level=config.get("logging_level", "INFO"))

    steps = [
        ("data_generation", generate_data.main),
        ("ingestion", ingest_to_staging.main),
        ("quality_checks", validate_data.main),
        ("transformation", staging_to_production.main),
        ("warehouse_load", load_warehouse.main),
        ("analytics", generate_analytics.execute_and_export),
    ]

    results = []
    status = "success"
    for step_name, func in steps:
        result = _run_with_retries(step_name, func, retries)
        results.append(result)
        if result["status"] != "success":
            status = "failed"
            break

    report = {
        "pipeline_name": "Ecommerce Analytics ETL",
        "execution_time": datetime.utcnow().isoformat(),
        "status": status,
        "steps": results,
    }

    with open(OUT / "pipeline_execution_report.json", "w") as f:
        json.dump(report, f, indent=4)

    return report


if __name__ == "__main__":
    print(run_pipeline())
