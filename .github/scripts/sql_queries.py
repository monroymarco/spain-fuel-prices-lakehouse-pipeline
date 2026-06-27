# ==========================================================
# PIPELINE METRICS
# ==========================================================

BRONZE_RECORDS = """
SELECT COUNT(*) AS total
FROM bronze_fuel_prices
"""

SILVER_RECORDS = """
SELECT COUNT(*) AS total
FROM silver_fuel_prices
"""

GOLD_TABLES = """
SELECT COUNT(*) AS total
FROM information_schema.tables
WHERE table_schema='default'
AND table_name LIKE 'gold_%'

"""

STATIONS_PROCESSED = """
SELECT COUNT(DISTINCT id_estacion)
FROM silver_fuel_prices
"""

LATEST_DATASET = """
SELECT DATE_FORMAT(MAX(fecha_ingestion),'dd/MM/yyyy HH:mm:ss')
FROM silver_fuel_prices
"""

# ==========================================================
# BUSINESS KPIs
# ==========================================================

CHEAPEST_PROVINCE = """
SELECT
    provincia,
    ROUND(precio_medio,3)
FROM gold_fuel_avg_price_by_province
ORDER BY precio_medio
LIMIT 1
"""

MOST_EXPENSIVE_PROVINCE = """
SELECT
    provincia,
    ROUND(precio_medio,3)
FROM gold_fuel_avg_price_by_province
ORDER BY precio_medio DESC
LIMIT 1
"""

CHEAPEST_STATION = """
SELECT
    rotulo,
    provincia,
    fuel_type,
    price
FROM gold_fuel_cheapest_ranking_nacional_top_10
ORDER BY ranking_nacional
LIMIT 1
"""

AVG_DIESEL = """
SELECT
    ROUND(AVG(price),3)
FROM silver_fuel_prices
WHERE fuel_type='gasoleo_a'
"""

AVG_GASOLINE_95 = """
SELECT
    ROUND(AVG(price),3)
FROM silver_fuel_prices
WHERE fuel_type='gasolina_95'
"""

AVG_GASOLINE_98 = """
SELECT
    ROUND(AVG(price),3)
FROM silver_fuel_prices
WHERE fuel_type='gasolina_98'
"""

BIGGEST_PRICE_DROP = """
SELECT
    rotulo,
    provincia,
    fuel_type,
    price_change
FROM gold_fuel_top_price_decreases
ORDER BY price_change
LIMIT 1
"""

BIGGEST_PRICE_INCREASE = """
SELECT
    rotulo,
    provincia,
    fuel_type,
    price_change
FROM gold_fuel_top_price_increases
ORDER BY price_change DESC
LIMIT 1
"""

# ==========================================================
# TOP 5
# ==========================================================

TOP5_CHEAPEST = """
SELECT
    rotulo,
    provincia,
    fuel_type,
    price
FROM gold_fuel_cheapest_ranking_nacional_top_10
ORDER BY ranking_nacional
LIMIT 5
"""

TOP5_EXPENSIVE = """
SELECT
    rotulo,
    provincia,
    fuel_type,
    price
FROM gold_fuel_expensive_ranking_nacional_top_10
ORDER BY ranking_nacional
LIMIT 5
"""