
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PruebaLecturaHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

try:
    print("\n=== LECTURA DE DATOS ORIGINALES ===")

    df_raw = spark.read.parquet(
        "hdfs://namenode:9000/smartcity/mongodb/raw"
    )

    total_raw = df_raw.count()
    print("Total de registros originales:", total_raw)

    print("\n=== LECTURA DE DATOS PROCESADOS ===")

    df_processed = spark.read.parquet(
        "hdfs://namenode:9000/smartcity/mongodb/processed"
    )

    total_processed = df_processed.count()
    print("Total de registros procesados:", total_processed)

    print("\n=== COMPARACIÓN ===")
    print("Originales:", total_raw)
    print("Procesados:", total_processed)
    print("Diferencia:", total_raw - total_processed)

finally:
    spark.stop()

