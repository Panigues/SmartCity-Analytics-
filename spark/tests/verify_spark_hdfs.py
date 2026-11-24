
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("VerifySparkHDFS")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)

try:
    ruta = "hdfs://namenode:9000/tmp/prueba_spark_hdfs"

    # Obtener el sistema de archivos de HDFS
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(ruta)
    filesystem = path.getFileSystem(hadoop_conf)

    # Limpiar únicamente la carpeta de esta prueba
    if filesystem.exists(path):
        filesystem.delete(path, True)

    # Escribir un mensaje de prueba en HDFS
    spark.sparkContext.parallelize(
        ["Conexion Spark-HDFS exitosa"]
    ).saveAsTextFile(ruta)

    # Leer el contenido desde HDFS
    resultado = spark.read.text(ruta).collect()

    print("\nRESULTADO DE LA PRUEBA:")
    for fila in resultado:
        print(fila["value"])

    print("\nConexion Spark-HDFS verificada correctamente.")

finally:
    spark.stop()
