# Databricks notebook source
from pyspark.sql.functions import col, expr

df_silver = spark.table("silver_fuel_prices")

# COMMAND ----------

from pyspark.sql.functions import col

df_silver = spark.table("silver_fuel_prices")

gold_fuel_prices_by_type = (
    df_silver
    .select(
        "fecha_dataset",
        "fecha_ingestion",
        "id_estacion",
        "provincia",
        "municipio",
        "direccion",
        "rotulo",
        "latitud",
        "longitud",
        "fuel_type",
        "price"
    )
    .filter(col("price").isNotNull())
)

# COMMAND ----------

gold_fuel_prices_by_type.groupBy("fuel_type").count().show()

# COMMAND ----------

gold_fuel_prices_by_type.write \
    .mode("append") \
    .format("delta") \
    .saveAsTable("gold_fuel_prices_by_type")

# COMMAND ----------

# DBTITLE 1,KPI gold_fuel_cheapest_station_by_type               ¿Cuál es la estación de servicio más barata de España para cada tipo de combustible?
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("fuel_type").orderBy("price")

gold_fuel_cheapest_station_by_type = (
    gold_fuel_prices_by_type
    .withColumn("rn", row_number().over(window_spec))
    .filter("rn = 1")
    .drop("rn")
)

# COMMAND ----------

gold_fuel_cheapest_station_by_type.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_fuel_cheapest_station_by_type")

# COMMAND ----------

# DBTITLE 1,KPI gold_fuel_cheapest_station_by_province.          ¿Cuál es la estación de servicio más barata de cada provincia para cada tipo de combustible?
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = Window.partitionBy("provincia", "fuel_type").orderBy(col("price").asc())

gold_fuel_cheapest_station_by_province = (
    gold_fuel_prices_by_type
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

gold_fuel_cheapest_station_by_province.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_fuel_cheapest_station_by_province")

# COMMAND ----------

# DBTITLE 1,KPI gold_fuel_avg_price_by_province.                  ¿Cuál es el precio medio de cada combustible en cada provincia de España?
from pyspark.sql.functions import avg, round

gold_fuel_avg_price_by_province = (
    gold_fuel_prices_by_type
    .groupBy("provincia", "fuel_type")
    .agg(
        round(avg("price"), 3).alias("precio_medio")
    )
)

# COMMAND ----------

gold_fuel_avg_price_by_province.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_fuel_avg_price_by_province")

# COMMAND ----------

# DBTITLE 1,KPI gold_fuel_avg_price_by_brand.                       ¿Cuál es el precio medio de cada combustible por marca de estación de servicio?
from pyspark.sql.functions import avg, round

gold_fuel_avg_price_by_brand = (
    gold_fuel_prices_by_type
    .groupBy("rotulo", "fuel_type")
    .agg(
        round(avg("price"), 3).alias("precio_medio")
    )
    .orderBy("fuel_type", "precio_medio")
)

# COMMAND ----------

gold_fuel_avg_price_by_brand.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_fuel_avg_price_by_brand")

# COMMAND ----------

# DBTITLE 1,gold_fuel_price_spread_by_province                    ¿Cuál es la diferencia entre la estación más barata y la más cara de cada provincia para cada combustible?
from pyspark.sql.functions import min, max, round, col

gold_fuel_price_spread_by_province = (
    gold_fuel_prices_by_type
    .groupBy("provincia", "fuel_type")
    .agg(
        round(min("price"), 3).alias("precio_minimo"),
        round(max("price"), 3).alias("precio_maximo")
    )
    .withColumn(
        "spread",
        round(col("precio_maximo") - col("precio_minimo"), 3)
    )
    .orderBy(col("spread").desc())
)

# COMMAND ----------

gold_fuel_price_spread_by_province.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_fuel_price_spread_by_province")

# COMMAND ----------

# DBTITLE 1,gold_fuel_most_expensive_station_by_type.   Estación Más Cara de España por Tipo de Combustible
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = Window.partitionBy("fuel_type").orderBy(col("price").desc())

gold_fuel_most_expensive_station_by_type = (
    gold_fuel_prices_by_type
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

gold_fuel_most_expensive_station_by_type.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_fuel_most_expensive_station_by_type")

# COMMAND ----------

spark.table("silver_fuel_prices").printSchema()

# COMMAND ----------

# DBTITLE 1,gold_fuel_cheapest_vs_province_avg                     Top 10 Estaciones Más Baratas vs Media Provincial “Esta tabla muestra las estaciones que están más por debajo del precio medio de su provincia.
media_provincial = (
    spark.table("gold_fuel_prices_by_type")
    .groupBy("provincia", "fuel_type")
    .agg(round(avg("price"), 3).alias("media_provincial"))
)

gold_fuel_cheapest_vs_province_avg = (
    spark.table("gold_fuel_prices_by_type")
    .join(media_provincial, ["provincia", "fuel_type"], "inner")
    .withColumn("diferencia_euros", round(col("price") - col("media_provincial"), 3))
    .withColumn("diferencia_pct", round((col("diferencia_euros") / col("media_provincial")) * 100, 2))
    .filter(col("diferencia_euros") < 0)
    .orderBy(col("diferencia_pct").asc())
    .select(
        "provincia",
        "municipio",
        "rotulo",
        "fuel_type",
        "price",
        "media_provincial",
        "diferencia_euros",
        "diferencia_pct"
    )
    .limit(10)
)

# COMMAND ----------

gold_fuel_cheapest_vs_province_avg.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_fuel_cheapest_vs_province_avg")

# COMMAND ----------

# DBTITLE 1,gold_fuel_cheapest_stations_nacional_top_10                                               gold_combustibles_barato_ranking_nacional_top_10
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col

window_top10 = Window.partitionBy("fuel_type").orderBy(col("price").asc())

gold_fuel_cheapest_ranking_nacional_top_10 = (
    gold_fuel_prices_by_type
    .withColumn("ranking_nacional", dense_rank().over(window_top10))
    .filter(col("ranking_nacional") <= 10)
    .select("ranking_nacional", "fuel_type", "price", "provincia", "municipio", "rotulo", "id_estacion")
    .orderBy("fuel_type", "ranking_nacional")
)

# COMMAND ----------

gold_fuel_cheapest_ranking_nacional_top_10.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_fuel_cheapest_ranking_nacional_top_10")

# COMMAND ----------

# DBTITLE 1,gold_fuel_expensive_ranking_nacional_top_10.                                               gold_combustibles_caro_ranking_nacional_top_10
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col

window_top10_expensive = Window.partitionBy("fuel_type").orderBy(col("price").desc())

gold_fuel_expensive_ranking_nacional_top_10 = (
    gold_fuel_prices_by_type
    .withColumn("ranking_nacional", dense_rank().over(window_top10_expensive))
    .filter(col("ranking_nacional") <= 10)
    .select("ranking_nacional", "fuel_type", "price", "provincia", "municipio", "rotulo", "id_estacion")
    .orderBy("fuel_type", "ranking_nacional")
)

# COMMAND ----------

gold_fuel_expensive_ranking_nacional_top_10.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_fuel_expensive_ranking_nacional_top_10")

# COMMAND ----------

# DBTITLE 1,gold_fuel_price_change_vs_previous_snapshot                        gold_variacion_precio_combustible_carga_anterior
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col

window_lag = Window.partitionBy("id_estacion", "fuel_type").orderBy("fecha_ingestion")

# COMMAND ----------

gold_fuel_price_change_vs_previous_snapshot = (
    gold_fuel_prices_by_type
    .withColumn(
        "previous_price",
        lag("price").over(window_lag)
    )
    .withColumn(
        "price_change",
        col("price") - col("previous_price")
    )
)

# COMMAND ----------

display(
    gold_fuel_price_change_vs_previous_snapshot
    .filter(col("previous_price").isNotNull()) # asi no me muestra los primeros registros que no tienen previous_price
)

# COMMAND ----------

gold_fuel_price_change_vs_previous_snapshot.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_fuel_price_change_vs_previous_snapshot")

# COMMAND ----------

# DBTITLE 1,gold_fuel_top_price_increases                                                       ¿Qué estaciones registraron los mayores aumentos de precio respecto a la carga anterior?
from pyspark.sql.functions import col

df_price_changes = spark.table("gold_fuel_price_change_vs_previous_snapshot")

gold_fuel_top_price_increases = (
    df_price_changes
    .filter(col("price_change") > 0)
    .orderBy(col("price_change").desc())
)

# COMMAND ----------

gold_fuel_top_price_increases.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_fuel_top_price_increases")

# COMMAND ----------

# DBTITLE 1,gold_fuel_top_price_decreases                                                      Mayores Disminuciones de Precio del Combustible
from pyspark.sql.functions import col

df_price_changes = spark.table("gold_fuel_price_change_vs_previous_snapshot")

gold_fuel_top_price_decreases = (
    df_price_changes
    .filter(col("price_change") < 0)
    .orderBy(col("price_change").asc())
)

# COMMAND ----------

gold_fuel_top_price_decreases.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("gold_fuel_top_price_decreases")