from pathlib import Path
import os

# Buscar el último JSON descargado
raw_path = Path("data/raw")

json_files = sorted(raw_path.glob("*.json"))

latest_file = json_files[-1].name if json_files else "No file found"

summary = f"""
# ⛽ Spain Fuel Prices Pipeline

## ✅ Execution Status
Success

## 📅 Execution Date
{os.popen("date").read().strip()}

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

# GitHub Summary
with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
    f.write(summary)

print("Execution Summary created.")
