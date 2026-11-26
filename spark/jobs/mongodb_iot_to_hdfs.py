from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    array,
    struct,
    explode,
    sha2,
    to_json,
    current_timestamp
)

HDFS_RAW = "hdfs://namenode:9000/smartcity/iot/raw"
HDFS_PROCESSED = "hdfs://namenode:9000/smartcity/iot/processed"

VARIABLES = [
    "co",
    "co2",
    "consumo_kwh",
    "demanda_kw",
    "humedad",
    "nivel_db",
    "no2",
    "ocupacion_pct",
    "pm10",
    "pm25",
    "presion",
    "temperatura",
    "tiempo_recorrido",
    "vehiculos",
    "velocidad_promedio",
    "viento",
    "voltaje"
]

spark = None

try:
    spark = (
        SparkSession.builder
        .appName("SmartCity-MongoDB-IoT-a-HDFS")
        .config(
            "spark.mongodb.read.connection.uri",
            "mongodb://host.docker.internal:27017/smartcity.iot_readings"
        )
        .config(
            "spark.mongodb.read.partitioner",
            "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner"
        )
        .config(
            "spark.hadoop.fs.defaultFS",
            "hdfs://namenode:9000"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )
        .getOrCreate()
    )

    print("\n=== LECTURA DE MONGODB IOT ===")

    df = spark.read.format("mongodb").load()

    print("\n=== ESQUEMA ORIGINAL ===")
    df.printSchema()

    print("\n=== ESCRITURA RAW EN HDFS ===")

    df.write \
        .mode("errorifexists") \
        .parquet(HDFS_RAW)

    print("Documentos originales guardados en HDFS.")

    print("\n=== LECTURA DE PARQUET RAW ===")

    df_raw = spark.read.parquet(HDFS_RAW)

    print("\n=== TRANSFORMACION A FORMATO LARGO ===")

    mediciones = array(*[
        struct(
            lit(variable).alias("variable"),
            col(f"mediciones.{variable}")
                .cast("double")
                .alias("valor")
        )
        for variable in VARIABLES
    ])

    df_procesado = (
        df_raw
        .select(
            col("sensor_id"),
            col("id_sensor"),
            col("tipo"),
            col("categoria"),
            col("zona.id").alias("id_zona"),
            col("zona.nombre").alias("nombre_zona"),
            col("timestamp"),
            col("ubicacion.coordinates")
                .getItem(0).alias("longitud"),
            col("ubicacion.coordinates")
                .getItem(1).alias("latitud"),
            explode(mediciones).alias("medicion")
        )
        .select(
            "sensor_id",
            "id_sensor",
            "tipo",
            "categoria",
            "id_zona",
            "nombre_zona",
            "timestamp",
            "longitud",
            "latitud",
            col("medicion.variable").alias("variable"),
            col("medicion.valor").alias("valor")
        )
        .filter(col("valor").isNotNull())
        .withColumn(
            "record_id",
            sha2(
                to_json(
                    struct(
                        col("sensor_id"),
                        col("timestamp"),
                        col("variable")
                    )
                ),
                256
            )
        )
        .withColumn("quality_status", lit("valid"))
        .withColumn("processed_at", current_timestamp())
        .repartition(8)
    )

    print("\n=== ESCRITURA PROCESSED EN HDFS ===")

    df_procesado.write \
        .mode("errorifexists") \
        .parquet(HDFS_PROCESSED)

    print("Mediciones procesadas guardadas en HDFS.")

    print("\n=== VERIFICACION DESDE HDFS ===")

    raw_verificado = (
        spark.read.parquet(HDFS_RAW).count()
    )

    processed_verificado = (
        spark.read.parquet(HDFS_PROCESSED).count()
    )

    print("Documentos RAW:", raw_verificado)
    print("Mediciones PROCESSED:", processed_verificado)

    if raw_verificado == 0:
        raise ValueError("No se encontraron documentos RAW.")

    if processed_verificado == 0:
        raise ValueError("No se encontraron mediciones PROCESSED.")

    print("\n=== PROCESAMIENTO IOT COMPLETADO ===")

except Exception as error:
    print("\nERROR DURANTE EL PROCESAMIENTO:")
    print(str(error))
    raise

finally:
    if spark is not None:
        spark.stop()
        print("\n=== SPARK FINALIZADO ===")
