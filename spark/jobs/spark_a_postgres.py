"""
SmartCity Analytics - ETL con Spark: HDFS -> limpieza -> PostgreSQL

Integración pedida por el enunciado: "datos limpios de Spark -> PostgreSQL".

  1. Extract:   lee las lecturas IoT desde HDFS (/smartcity/lecturas, particionadas
                por anio=/mes=) y la tabla sensor desde PostgreSQL por JDBC.
  2. Transform: limpieza (nulos, duplicados, rangos imposibles), detección de
                anomalías con una desviación robusta por sensor, variable y hora
                del día, y validación contra las anomalías inyectadas por el
                generador.
  3. Load:      escribe en PostgreSQL por JDBC:
                  - lectura_diaria          agregados diarios limpios
                  - indicador_zona_mensual  indicadores por zona y mes
                  - anomalia_detectada      lecturas anómalas encontradas
                y guarda una copia de los indicadores en HDFS (Parquet).

Uso (PowerShell, desde la raíz del repo, con el contenedor spark arriba):
  docker exec -it spark /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g `
      --packages org.postgresql:postgresql:42.7.4 --conf spark.jars.ivy=/tmp/.ivy `
      /opt/spark-apps/jobs/spark_a_postgres.py
"""
import argparse
import os
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

ESQUEMA = StructType([
    StructField("timestamp", StringType()),
    StructField("zone_id", StringType()),
    StructField("domain", StringType()),
    StructField("tipo", StringType()),
    StructField("sensor_id", StringType()),
    StructField("variable", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
])

# Variables que nunca pueden ser negativas y rangos físicamente posibles
RANGOS = {
    "ocupacion_pct": (0, 100), "humedad": (0, 100), "temperatura": (-40, 50),
    "nivel_db": (0, 140), "velocidad_promedio": (0, 200),
}
UMBRAL_ANOMALIA = 6.0   # desviaciones robustas desde la mediana


def parametros():
    db = os.environ.get("POSTGRES_DB", "smartcity")
    ap = argparse.ArgumentParser()
    ap.add_argument("--entrada", default="hdfs://namenode:9000/smartcity/lecturas")
    ap.add_argument("--eventos", default="hdfs://namenode:9000/smartcity/eventos/anomalias_inyectadas.csv")
    ap.add_argument("--salida-hdfs", default="hdfs://namenode:9000/smartcity/results/spark")
    ap.add_argument("--jdbc", default=f"jdbc:postgresql://postgres:5432/{db}")
    return ap.parse_args()


def main():
    args = parametros()
    inicio = time.time()
    jdbc_props = {
        "user": os.environ.get("POSTGRES_USER", "smartcity"),
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
        "driver": "org.postgresql.Driver",
    }

    spark = (SparkSession.builder
             .appName("SmartCity ETL HDFS -> PostgreSQL")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "48")
             .config("spark.hadoop.dfs.replication", "2")   # el clúster tiene 2 DataNodes
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------ EXTRACT
    print("\n=== 1. EXTRACT ===")
    crudo = (spark.read.option("header", True).schema(ESQUEMA)
             .csv(args.entrada)          # descubre las particiones anio= y mes=
             .withColumn("fecha_hora", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'")))
    total = crudo.count()
    print(f"Lecturas leídas desde HDFS: {total:,}")

    sensores = (spark.read.jdbc(args.jdbc, "sensor", properties=jdbc_props)
                .select("id_sensor", F.col("nombre").alias("sensor_id")))
    print(f"Sensores leídos desde PostgreSQL: {sensores.count()}")

    # ---------------------------------------------------------- TRANSFORM
    print("\n=== 2. TRANSFORM: limpieza ===")
    limpio = crudo.dropna(subset=["fecha_hora", "sensor_id", "variable", "value"])
    sin_nulos = limpio.count()
    limpio = limpio.dropDuplicates(["sensor_id", "fecha_hora", "variable"])
    sin_duplicados = limpio.count()

    condicion = F.col("value") >= 0
    for var, (lo, hi) in RANGOS.items():
        condicion = F.when(F.col("variable") == var,
                           F.col("value").between(lo, hi)).otherwise(condicion)
    validos = limpio.withColumn("valido", condicion)
    fuera_rango = validos.filter(~F.col("valido")).count()
    limpio = validos.filter("valido").drop("valido")

    # Asociar el id_sensor de PostgreSQL (join con datos relacionales)
    limpio = limpio.join(F.broadcast(sensores), "sensor_id", "inner").cache()
    final = limpio.count()
    print(f"  con nulos descartados:      {total - sin_nulos:,}")
    print(f"  duplicados descartados:     {sin_nulos - sin_duplicados:,}")
    print(f"  fuera de rango físico:      {fuera_rango:,}")
    print(f"  lecturas limpias:           {final:,}")

    print("\n=== 2. TRANSFORM: detección de anomalías ===")
    # Comportamiento normal de cada sensor/variable a cada hora del día:
    # mediana y desviación absoluta mediana (MAD), robustas a los picos.
    limpio = limpio.withColumn("hora", F.hour("fecha_hora"))
    claves = ["sensor_id", "variable", "hora"]
    normal = limpio.groupBy(claves).agg(F.percentile_approx("value", 0.5).alias("mediana"))
    desv = (limpio.join(normal, claves)
            .withColumn("dev_abs", F.abs(F.col("value") - F.col("mediana")))
            .groupBy(claves).agg(F.percentile_approx("dev_abs", 0.5).alias("mad")))
    puntuado = (limpio.join(normal, claves).join(desv, claves)
                  .withColumn("escala", F.greatest(
                    F.col("mad") * 1.4826,
                    F.abs(F.col("mediana")) * 0.05,
                    # Conteos (vehículos): variación mínima tipo Poisson, raíz de la media.
                    # Evita dividir entre ~0 cuando a esa hora casi siempre pasan 0 vehículos.
                    F.when(F.col("variable") == "vehiculos", F.sqrt(F.col("mediana") + 1))
                     .otherwise(F.lit(1e-3))))
                .withColumn("puntaje", (F.col("value") - F.col("mediana")) / F.col("escala"))
                .withColumn("anomala", F.abs(F.col("puntaje")) > UMBRAL_ANOMALIA)
                .cache())
    anomalias = puntuado.filter("anomala")
    n_anom = anomalias.count()
    print(f"  anomalías detectadas: {n_anom:,} ({100 * n_anom / final:.3f} % de las lecturas)")

    # Validación contra las anomalías que el generador inyectó a propósito
    try:
        inyectadas = (spark.read.option("header", True).csv(args.eventos)
                      .select(F.col("sensor").alias("sensor_id"), "variable",
                              F.to_timestamp("timestamp").alias("fecha_hora")))
        n_iny = inyectadas.count()
        acertadas = inyectadas.join(anomalias.select("sensor_id", "variable", "fecha_hora"),
                                    ["sensor_id", "variable", "fecha_hora"]).count()
        print(f"  inyectadas por el generador: {n_iny:,} · detectadas: {acertadas:,} "
              f"({100 * acertadas / max(n_iny, 1):.1f} %)")
    except Exception as e:  # el archivo de eventos es opcional
        print(f"  (sin validación contra anomalías inyectadas: {e.__class__.__name__})")

    normales = puntuado.filter(~F.col("anomala"))

    # Agregados diarios limpios (sin anomalías)
    diario = (normales.groupBy("id_sensor", F.to_date("fecha_hora").alias("fecha"), "variable")
              .agg(F.round(F.avg("value"), 4).alias("promedio"),
                   F.round(F.min("value"), 4).alias("minimo"),
                   F.round(F.max("value"), 4).alias("maximo"),
                   F.count("*").cast("int").alias("cantidad")))

    # Indicadores urbanos por zona y mes
    indicadores = (puntuado.groupBy(F.col("zone_id").alias("id_zona"), "anio", "mes", "variable")
                   .agg(F.round(F.avg(F.when(~F.col("anomala"), F.col("value"))), 4).alias("promedio"),
                        F.round(F.max(F.when(~F.col("anomala"), F.col("value"))), 4).alias("maximo"),
                        F.count("*").cast("int").alias("lecturas"),
                        F.sum(F.col("anomala").cast("int")).cast("int").alias("anomalias")))

    detalle_anom = anomalias.select(
        "id_sensor", "fecha_hora", "variable",
        F.round("value", 4).alias("valor"), F.round("mediana", 4).alias("esperado"),
        F.round("puntaje", 2).alias("puntaje"))

    # --------------------------------------------------------------- LOAD
    print("\n=== 3. LOAD -> PostgreSQL (JDBC) ===")

    def escribir(df, tabla):
        # overwrite + truncate: vacía la tabla con TRUNCATE y conserva su
        # definición (llaves, CHECK, índices) en lugar de recrearla
        (df.write.mode("overwrite").option("truncate", "true")
         .option("batchsize", 10000).jdbc(args.jdbc, tabla, properties=jdbc_props))
        print(f"  {tabla:<24} {spark.read.jdbc(args.jdbc, tabla, properties=jdbc_props).count():>9,} filas")

    escribir(diario, "lectura_diaria")
    escribir(indicadores, "indicador_zona_mensual")
    escribir(detalle_anom, "anomalia_detectada")

    print("\n=== 3. LOAD -> HDFS (Parquet) ===")
    try:
        indicadores.write.mode("overwrite").partitionBy("anio").parquet(
            f"{args.salida_hdfs}/indicador_zona_mensual")
        print(f"  {args.salida_hdfs}/indicador_zona_mensual")
    except Exception as e:
        print(f"  (no se pudo escribir en HDFS: {e.__class__.__name__})")

    print(f"\nETL completado en {time.time() - inicio:.0f} s")
    spark.stop()


if __name__ == "__main__":
    main()
