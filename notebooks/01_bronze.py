# Databricks notebook source
from pyspark.sql.functions import input_file_name

raw_path = "/Volumes/workspace/default/fuel_prices/raw"

files = dbutils.fs.ls(raw_path)

latest_file = sorted(
    [f.path for f in files if f.path.endswith(".json")]
)[-1]

print(f"Procesando: {latest_file}")

df_raw = (
    spark.read
    .option("multiline", "true")
    .json(latest_file)
)

# COMMAND ----------

from pyspark.sql.functions import explode, col, current_timestamp

df_bronze = (
    df_raw
    .select(
        col("Fecha").alias("fecha_dataset"),
        explode("ListaEESSPrecio").alias("estacion")
    )
    .select(
        "fecha_dataset",
        current_timestamp().alias("fecha_ingestion"),
        col("estacion.IDEESS").alias("id_estacion"),
        col("estacion.Provincia").alias("provincia"),
        col("estacion.Municipio").alias("municipio"),
        col("estacion.Dirección").alias("direccion"),
        col("estacion.Rótulo").alias("rotulo"),
        col("estacion.Latitud").alias("latitud"),
        col("estacion.`Longitud (WGS84)`").alias("longitud"),
        col("estacion.`Precio Gasoleo A`").alias("precio_gasoleo_a"),
        col("estacion.`Precio Gasolina 95 E5`").alias("precio_gasolina_95"),
        col("estacion.`Precio Gasolina 98 E5`").alias("precio_gasolina_98")
    )
)

# COMMAND ----------

df_bronze.write \
    .mode("append") \
    .format("delta") \
    .saveAsTable("bronze_fuel_prices")