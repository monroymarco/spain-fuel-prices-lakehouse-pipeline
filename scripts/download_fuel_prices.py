import json
from datetime import datetime

import requests

URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"

response = requests.get(URL, timeout=60)
response.raise_for_status()

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