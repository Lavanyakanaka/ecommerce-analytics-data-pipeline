import json
from datetime import datetime
from pathlib import Path

from scripts.db_connection import get_connection


REPORT_PATH = Path("data/processed")
REPORT_PATH.mkdir(parents=True, exist_ok=True)


def _load_queries() -> list:
    query_path = Path("sql/queries/monitoring_queries.sql")
    if not query_path.exists():
        return []
    content = query_path.read_text(encoding="utf-8")
    return [q.strip() for q in content.split(";") if q.strip()]


def run_monitoring() -> dict:
    connection = get_connection()
    results = []
    with connection.cursor() as cur:
        for query in _load_queries():
            cur.execute(query)
            rows = cur.fetchall()
            results.append({"query": query, "rows": rows})

    pipeline_report = None
    report_file = REPORT_PATH / "pipeline_execution_report.json"
    if report_file.exists():
        pipeline_report = json.loads(report_file.read_text(encoding="utf-8"))

    report = {
        "timestamp": datetime.utcnow().isoformat(),
        "pipeline_report": pipeline_report,
        "monitoring_results": results,
    }

    with open(REPORT_PATH / "monitoring_report.json", "w") as f:
        json.dump(report, f, indent=4)

    connection.close()
    return report


if __name__ == "__main__":
    print(run_monitoring())
