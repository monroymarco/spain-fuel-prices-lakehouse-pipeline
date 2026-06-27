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

    print("Sending SQL statement...")

    response = requests.post(
        f"{HOST}/api/2.0/sql/statements/",
        headers=HEADERS,
        json={
            "warehouse_id": WAREHOUSE,
            "statement": statement
        },
        timeout=30
    )

    print("POST:", response.status_code)

    response.raise_for_status()

    data = response.json()

    print(json.dumps(data, indent=2))

    statement_id = data["statement_id"]

    while True:

        print("Checking status...")

        status = requests.get(
            f"{HOST}/api/2.0/sql/statements/{statement_id}",
            headers=HEADERS,
            timeout=30
        )

        print("GET:", status.status_code)

        status.raise_for_status()

        result = status.json()

        print(json.dumps(result, indent=2))

        state = result["status"]["state"]

        print("STATE =", state)

        if state == "SUCCEEDED":
            return result

        if state in ("FAILED", "CANCELED"):
            raise Exception(json.dumps(result, indent=2))

        time.sleep(2)


# --------------------------
# Test query
# --------------------------

result = execute_sql("""
SELECT COUNT(*) AS total
FROM bronze_fuel_prices
""")

print(json.dumps(result, indent=2))