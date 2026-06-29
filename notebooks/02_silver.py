# Databricks notebook source
from pyspark.sql.functions import *

df_bronze = spark.table("bronze_fuel_prices")

# COMMAND ----------

df_silver = (
    df_bronze
    .withColumn("fecha_dataset", to_timestamp("fecha_dataset", "dd/MM/yyyy H:mm:ss"))
    .withColumn("provincia", upper(trim(col("provincia"))))
    .withColumn("municipio", upper(trim(col("municipio"))))
    .withColumn("rotulo", upper(trim(col("rotulo"))))
    .withColumn("latitud", expr("try_cast(replace(latitud, ',', '.') as double)"))
    .withColumn("longitud", expr("try_cast(replace(longitud, ',', '.') as double)"))
    .withColumn("precio_gasoleo_a", expr("try_cast(replace(precio_gasoleo_a, ',', '.') as double)"))
    .withColumn("precio_gasolina_95", expr("try_cast(replace(precio_gasolina_95, ',', '.') as double)"))
    .withColumn("precio_gasolina_98", expr("try_cast(replace(precio_gasolina_98, ',', '.') as double)"))
)

# COMMAND ----------

from pyspark.sql.functions import expr

df_silver = (
    df_silver
    .selectExpr(
        "fecha_dataset",
        "fecha_ingestion",
        "id_estacion",
        "provincia",
        "municipio",
        "direccion",
        "rotulo",
        "latitud",
        "longitud",
        """
        stack(
            3,
            'gasoleo_a', precio_gasoleo_a,
            'gasolina_95', precio_gasolina_95,
            'gasolina_98', precio_gasolina_98
        ) as (fuel_type, price)
        """
    )
    .filter("price IS NOT NULL")
)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS silver_fuel_prices")

# COMMAND ----------

df_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("silver_fuel_prices")