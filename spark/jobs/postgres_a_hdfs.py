"""
Exportación PostgreSQL -> HDFS (CSV) con Spark

Cubre el requisito de "Exportaciones CSV" en HDFS: Spark lee las tablas
de PostgreSQL por JDBC y las guarda como CSV en
/smartcity/exports/postgres/<tabla>/ (un archivo por tabla, con encabezado).

Uso (PowerShell, desde la raíz del repo, con el contenedor spark arriba):
  docker exec -it spark /opt/spark/bin/spark-submit --master "local[*]" --packages org.postgresql:postgresql:42.7.4 --conf spark.jars.ivy=/tmp/.ivy /opt/spark-apps/jobs/postgres_a_hdfs.py
"""
import argparse
import os

from pyspark.sql import SparkSession

TABLAS = ["categoria", "tipo_sensor", "zona", "ubicacion", "organismo", "usuario", "sensor",
          "lectura_diaria", "indicador_zona_mensual", "anomalia_detectada"]


def main():
    db = os.environ.get("POSTGRES_DB", "smartcity")
    ap = argparse.ArgumentParser()
    ap.add_argument("--jdbc", default=f"jdbc:postgresql://postgres:5432/{db}")
    ap.add_argument("--salida", default="hdfs://namenode:9000/smartcity/exports/postgres")
    args = ap.parse_args()

    props = {"user": os.environ.get("POSTGRES_USER", os.environ.get("PG_USER", "smartcity")),
             "password": os.environ.get("POSTGRES_PASSWORD", os.environ.get("PG_PASSWORD", "")),
             "driver": "org.postgresql.Driver"}

    spark = (SparkSession.builder.appName("SmartCity PostgreSQL -> HDFS")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.hadoop.dfs.replication", "2")     # el clúster tiene 2 DataNodes
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print(f"\nExportando {len(TABLAS)} tablas de PostgreSQL a {args.salida}\n")
    for tabla in TABLAS:
        try:
            df = spark.read.jdbc(args.jdbc, tabla, properties=props)
        except Exception as e:
            print(f"  {tabla:<24} omitida ({e.__class__.__name__}: la tabla no existe)")
            continue
        filas = df.count()
        (df.coalesce(1).write.mode("overwrite").option("header", True)
         .csv(f"{args.salida}/{tabla}"))
        print(f"  {tabla:<24} {filas:>9,} filas  ->  {args.salida}/{tabla}/")

    print("\nExportación completada.")
    spark.stop()


if __name__ == "__main__":
    main()
