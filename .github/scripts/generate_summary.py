import os
from datetime import datetime

from databricks_sql import (
    get_single_value,
    get_single_row,
    get_table
)

from sql_queries import *

from report_builder import build_report


def main():

    metrics = {}

    # ======================================================
    # EXECUTION
    # ======================================================

    metrics["status"] = "SUCCESS"

    metrics["run_id"] = os.environ.get("RUN_ID", "-")

    metrics["execution_date"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    metrics["duration"] = "Calculated by Databricks"

    # ======================================================
    # PIPELINE METRICS
    # ======================================================

    metrics["bronze"] = int(get_single_value(BRONZE_RECORDS))

    metrics["silver"] = int(get_single_value(SILVER_RECORDS))

    metrics["gold_tables"] = int(get_single_value(GOLD_TABLES))

    metrics["stations"] = int(get_single_value(STATIONS_PROCESSED))

    metrics["latest_dataset"] = get_single_value(LATEST_DATASET)

    # ======================================================
    # BUSINESS KPIs
    # ======================================================

    province = get_single_row(CHEAPEST_PROVINCE)

    metrics["cheapest_province"] = (
        f"{province[0]} ({province[1]} €/L)"
    )

    station = get_single_row(CHEAPEST_STATION)

    metrics["cheapest_station"] = (
        f"{station[0]} - {station[1]}"
    )
    from databricks_sql import get_table
    
    print(get_table("SELECT current_catalog(), current_schema()"))
    
    print(get_table("DESCRIBE TABLE silver_fuel_prices"))
    metrics["avg_diesel"] = get_single_value(AVG_DIESEL)

    metrics["avg_gas95"] = get_single_value(AVG_GASOLINE_95)

    metrics["avg_gas98"] = get_single_value(AVG_GASOLINE_98)

    drop = get_single_row(BIGGEST_PRICE_DROP)

    metrics["biggest_drop"] = (
        f"{drop[3]} ({drop[0]})"
    )

    # ======================================================
    # FILE
    # ======================================================

    metrics["processed_file"] = "Generated automatically"

    # ======================================================
    # BUILD REPORT
    # ======================================================

    report = build_report(metrics)

    with open(os.environ["GITHUB_STEP_SUMMARY"], "w") as f:
        f.write(report)

    print("Execution Summary generated successfully.")


if __name__ == "__main__":
    main()