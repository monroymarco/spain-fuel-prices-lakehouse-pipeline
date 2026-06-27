from datetime import datetime


def build_report(metrics):

    report = f"""
# ⛽ Spain Fuel Prices Pipeline

---

# ✅ EXECUTION

| Metric | Value |
|--------|-------|
| Status | {metrics["status"]} |
| Run ID | {metrics["run_id"]} |
| Duration | {metrics["duration"]} |
| Execution Date | {metrics["execution_date"]} |

---

# 📊 BUSINESS KPIs

| KPI | Value |
|-----|-------|
| 🏆 Cheapest Province | {metrics["cheapest_province"]} |
| ⛽ Cheapest Station | {metrics["cheapest_station"]} |
| 💰 Average Diesel A | {metrics["avg_diesel"]} €/L |
| ⛽ Average Gasoline 95 | {metrics["avg_gas95"]} €/L |
| ⛽ Average Gasoline 98 | {metrics["avg_gas98"]} €/L |
| 📉 Biggest Price Drop | {metrics["biggest_drop"]} €/L |

---

# ⚙️ PIPELINE METRICS

| Metric | Value |
|--------|-------|
| Bronze Records | {metrics["bronze"]:,} |
| Silver Records | {metrics["silver"]:,} |
| Gold Tables | {metrics["gold_tables"]} |
| Stations Processed | {metrics["stations"]:,} |
| Latest Dataset | {metrics["latest_dataset"]} |

---

# 📦 PROCESSED FILE

{metrics["processed_file"]}

---

# 🚀 TECHNOLOGIES

- Python
- GitHub Actions
- Databricks Workflows
- Databricks SQL Warehouse
- PySpark
- Delta Lake
- Medallion Architecture

---

_Report generated automatically by GitHub Actions._
"""

    return report