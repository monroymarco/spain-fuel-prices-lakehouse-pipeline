import os
import requests
import json

HOST = os.environ["DATABRICKS_HOST"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE = os.environ["DATABRICKS_WAREHOUSE_ID"]

response = requests.post(
    f"{HOST}/api/2.0/sql/statements/",
    headers={
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    },
    json={
        "warehouse_id": WAREHOUSE,
        "statement": "SELECT COUNT(*) AS total FROM bronze_fuel_prices"
    }
)

print(response.status_code)
print(json.dumps(response.json(), indent=2))

with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
    f.write("# SQL Warehouse Test\n")
    f.write("Consulta enviada correctamente.\n")