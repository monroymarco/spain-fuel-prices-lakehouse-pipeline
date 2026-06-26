import json
import time
from datetime import datetime

import requests

URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"

response = None

for attempt in range(5):
    try:
        print(f"Download attempt {attempt + 1}/5...")

        response = requests.get(
            URL,
            timeout=60,
            headers={
                "User-Agent": "Mozilla/5.0"
            },
        )

        response.raise_for_status()
        break

    except requests.exceptions.RequestException as e:
        print(f"Attempt failed: {e}")

        if attempt == 4:
            raise

        print("Retrying in 10 seconds...")
        time.sleep(10)

data = response.json()

fecha_dataset = data["Fecha"]
numero_estaciones = len(data["ListaEESSPrecio"])

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

file_name = f"data/raw/fuel_prices_{timestamp}.json"

with open(file_name, "w", encoding="utf-8") as file:
    json.dump(data, file, ensure_ascii=False, indent=4)

print("\n=== EXTRACCIÓN COMPLETADA ===")
print(f"Fecha dataset      : {fecha_dataset}")
print(f"Estaciones         : {numero_estaciones}")
print(f"Archivo generado   : {file_name}")