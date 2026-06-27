import os
import time
import json
import requests

HOST = os.environ["DATABRICKS_HOST"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE = os.environ["DATABRICKS_WAREHOUSE_ID"]

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}


def execute_sql(statement):
    # Enviar consulta
    response = requests.post(
        f"{HOST}/api/2.0/sql/statements/",
        headers=HEADERS,
        json={
            "warehouse_id": WAREHOUSE,
            "statement": statement
        }
    )

    response.raise_for_status()

    data = response.json()
    statement_id = data["statement_id"]

    # Esperar a que termine
    while True:

        status = requests.get(
            f"{HOST}/api/2.0/sql/statements/{statement_id}",
            headers=HEADERS
        )

        status.raise_for_status()

        result = status.json()

        state = result["status"]["state"]

        print("SQL State:", state)

        if state == "SUCCEEDED":
            return result

        if state in ("FAILED", "CANCELED"):
            raise Exception(json.dumps(result, indent=2))

        time.sleep(2)
        
        
        
        result = execute_sql("""
            SELECT COUNT(*) AS total
            FROM bronze_fuel_prices
            """)

        print(json.dumps(result, indent=2))