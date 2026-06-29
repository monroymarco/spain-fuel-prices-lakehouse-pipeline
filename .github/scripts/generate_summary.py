from pathlib import Path
from datetime import datetime
import os

raw_path = Path("data/raw")
json_files = sorted(raw_path.glob("*.json"))

latest_file = json_files[-1].name if json_files else "No file found"

summary = f"""
# ⛽ Spain Fuel Prices Pipeline

## ✅ Execution Status
SUCCESS

## 🆔 Run ID
{os.environ.get("RUN_ID", "-")}

## 📅 Execution Date
{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}

## 📦 Processed File
{latest_file}

## 🔄 Pipeline

- ✅ Download Fuel Prices
- ✅ Upload to Databricks Volume
- ✅ Bronze
- ✅ Silver
- ✅ Gold

## 🚀 Trigger

GitHub Actions → Databricks Workflow
"""

with open(os.environ["GITHUB_STEP_SUMMARY"], "w") as f:
    f.write(summary)

print("Execution Summary generated successfully.")