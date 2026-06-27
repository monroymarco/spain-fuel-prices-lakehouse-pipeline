from pathlib import Path
from datetime import datetime
import os
import requests

# ==========================
# Environment variables
# ==========================

HOST = os.environ["DATABRICKS_HOST"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
RUN_ID = os.environ["RUN_ID"]

# ==========================
# Databricks Job information
# ==========================

response = requests.get(
    f"{HOST}/api/2.1/jobs/runs/get",
    headers={"Authorization": f"Bearer {TOKEN}"},
    params={"run_id": RUN_ID},
)

job = response.json()

state = job["state"]["result_state"]
life_cycle = job["state"]["life_cycle_state"]

start_time = job["start_time"]
end_time = job["end_time"]

duration_seconds = int((end_time - start_time) / 1000)

minutes = duration_seconds // 60
seconds = duration_seconds % 60

execution_time = datetime.fromtimestamp(
    end_time / 1000
).strftime("%d/%m/%Y %H:%M:%S")

# ==========================
# Latest processed JSON
# ==========================

raw_path = Path("data/raw")

json_files = sorted(raw_path.glob("*.json"))

latest_file = (
    json_files[-1].name
    if json_files
    else "No file found"
)

# ==========================
# Summary
# ==========================

summary = f"""
# ⛽ Spain Fuel Prices Pipeline

## ✅ Execution Status
**{state}**

## 🆔 Run ID
{RUN_ID}

## ⏱ Duration
{minutes} min {seconds} sec

## 📅 Execution Date
{execution_time}

## 📦 Processed File
{latest_file}

## ⚙️ Databricks Job

- Life Cycle: **{life_cycle}**
- Result: **{state}**

## 🔄 Pipeline

- ✅ Download Fuel Prices
- ✅ Upload to Databricks Volume
- ✅ Bronze
- ✅ Silver
- ✅ Gold

## 🚀 Trigger

GitHub Actions → Databricks Workflow
"""

with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
    f.write(summary)

print("Execution Summary created.")