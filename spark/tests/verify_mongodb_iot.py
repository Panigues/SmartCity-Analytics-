from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("VerificarMongoDBIoT")
    .config(
        "spark.mongodb.read.connection.uri",
        "mongodb://host.docker.internal:27017/smartcity.iot_readings"
    )
    .getOrCreate()
)

try:
    print("\n=== LECTURA DE IOT_READINGS ===")

    df = spark.read.format("mongodb").load()

    print("\n=== ESQUEMA ===")
    df.printSchema()

    print("\n=== MUESTRA DE DOCUMENTOS ===")
    df.show(5, truncate=False)


finally:
    spark.stop()
