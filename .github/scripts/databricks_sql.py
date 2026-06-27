import os
import time
import requests

# ==========================
# Configuration
# ==========================

HOST = os.environ["DATABRICKS_HOST"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE = os.environ["DATABRICKS_WAREHOUSE_ID"]

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# ==========================
# Internal functions
# ==========================

def _submit_statement(statement: str) -> str:
    """
    Sends a SQL statement to Databricks SQL Warehouse.

    Returns:
        statement_id
    """

    response = requests.post(
        f"{HOST}/api/2.0/sql/statements/",
        headers=HEADERS,
        json={
            "warehouse_id": WAREHOUSE,
            "catalog": "workspace",
            "schema": "default",
            "statement": statement
        },
        timeout=30
    )

    response.raise_for_status()

    data = response.json()

    return data["statement_id"]


def _wait_for_completion(statement_id: str) -> dict:
    """
    Waits until the SQL statement finishes.

    Returns:
        Complete JSON response.
    """

    while True:

        response = requests.get(
            f"{HOST}/api/2.0/sql/statements/{statement_id}",
            headers=HEADERS,
            timeout=30
        )

        response.raise_for_status()

        result = response.json()

        state = result["status"]["state"]

        if state == "SUCCEEDED":
            return result

        if state in ("FAILED", "CANCELED"):

            raise Exception(
                f"SQL Statement failed:\n{result}"
            )

        time.sleep(2)

# ==========================
# Public API
# ==========================

def execute_sql(statement: str) -> dict:
    """
    Executes a SQL statement and returns
    the complete Databricks response.
    """

    statement_id = _submit_statement(statement)

    return _wait_for_completion(statement_id)


def get_single_value(statement: str):
    """
    Returns the first value of the first row.

    Example:

        SELECT COUNT(*) FROM table

    Returns:
        12345
    """

    result = execute_sql(statement)

    return result["result"]["data_array"][0][0]


def get_single_row(statement: str):
    """
    Returns the first row.

    Example:

        SELECT provincia, precio
        ...

    Returns:

        ["LLEIDA", "1.428"]
    """

    result = execute_sql(statement)

    return result["result"]["data_array"][0]


def get_table(statement: str):
    """
    Returns all rows.

    Useful for TOP 10 reports.
    """

    result = execute_sql(statement)

    return result["result"]["data_array"]