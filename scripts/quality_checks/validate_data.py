import json
from pathlib import Path

from scripts.db_connection import get_connection


OUT = Path("data/processed")
OUT.mkdir(parents=True, exist_ok=True)


def check_null_values(connection, schema: str) -> dict:
    result = {}
    with connection.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.customers WHERE email IS NULL")
        result["customers_email_nulls"] = cur.fetchone()[0] == 0
        cur.execute(f"SELECT COUNT(*) FROM {schema}.products WHERE price IS NULL")
        result["products_price_nulls"] = cur.fetchone()[0] == 0
        cur.execute(f"SELECT COUNT(*) FROM {schema}.transactions WHERE total_amount IS NULL")
        result["transactions_total_nulls"] = cur.fetchone()[0] == 0
        cur.execute(f"SELECT COUNT(*) FROM {schema}.transaction_items WHERE unit_price IS NULL")
        result["items_price_nulls"] = cur.fetchone()[0] == 0
    return result


def check_duplicates(connection, schema: str) -> dict:
    result = {}
    with connection.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.customers GROUP BY customer_id HAVING COUNT(*) > 1")
        result["customers_duplicates"] = cur.fetchone() is None
        cur.execute(f"SELECT COUNT(*) FROM {schema}.products GROUP BY product_id HAVING COUNT(*) > 1")
        result["products_duplicates"] = cur.fetchone() is None
    return result


def check_referential_integrity(connection, schema: str) -> dict:
    result = {}
    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM {schema}.transactions t
            LEFT JOIN {schema}.customers c ON t.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
            """
        )
        result["transactions_fk_valid"] = cur.fetchone()[0] == 0
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM {schema}.transaction_items ti
            LEFT JOIN {schema}.transactions t ON ti.transaction_id = t.transaction_id
            WHERE t.transaction_id IS NULL
            """
        )
        result["items_tx_fk_valid"] = cur.fetchone()[0] == 0
    return result


def check_data_ranges(connection, schema: str) -> dict:
    result = {}
    with connection.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.transactions WHERE total_amount <= 0")
        result["total_amount_valid"] = cur.fetchone()[0] == 0
        cur.execute(f"SELECT COUNT(*) FROM {schema}.transaction_items WHERE quantity <= 0")
        result["quantity_valid"] = cur.fetchone()[0] == 0
    return result


def calculate_quality_score(check_results: dict) -> float:
    return round(
        sum(value is True for value in check_results.values()) / len(check_results) * 100,
        2,
    )


def main(schema: str = "production") -> dict:
    connection = get_connection()
    checks = {}
    checks.update(check_null_values(connection, schema))
    checks.update(check_duplicates(connection, schema))
    checks.update(check_referential_integrity(connection, schema))
    checks.update(check_data_ranges(connection, schema))
    score = calculate_quality_score(checks)

    report = {"schema": schema, "checks": checks, "quality_score": score}
    with open(OUT / "quality_report.json", "w") as f:
        json.dump(report, f, indent=4)

    connection.close()
    return report


if __name__ == "__main__":
    print(main())
