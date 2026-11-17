

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    count,
    lit,
    current_timestamp,
    sha2,
    to_json,
    struct,
    to_timestamp
)


# Estado inicial del proceso
estado_proceso = "failed"

# Contadores iniciales
total_registros = 0
registros_limpios = 0
duplicados_eliminados = 0

# En esta ejecución no se escriben datos en cleaned_data
registros_escritos = 0

spark = None


try:


    # INICIAR SPARK


    spark = (
        SparkSession.builder
        .appName("SmartCity")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0"
        )
        .config(
            "spark.mongodb.read.connection.uri",
            "mongodb://host.docker.internal:27017/smartcity.pollution_measurements"
        )
        .config(
            "spark.mongodb.write.connection.uri",
            "mongodb://host.docker.internal:27017/smartcity.cleaned_data"
        )
        .getOrCreate()
    )

    print("\n=== SPARK INICIADO CORRECTAMENTE ===")



    # LEER DATOS DESDE MONGODB


    print("\n=== LECTURA DE DATOS ===")
    df = spark.read.format("mongodb").load()
    total_registros = df.count()
    print("Total de registros:", total_registros)



    # ESTRUCTURA DE LOS DATOS

    print("\n=== ESQUEMA DE LOS DATOS ===")
    df.printSchema()



    # VALORES NULOS


    print("\n=== VALORES NULOS POR CAMPO ===")
    null_counts = df.select([
        count(when(col(c).isNull(), 1)).alias(c)
        for c in df.columns
    ])
    null_counts.show(truncate=False)



    # MUESTRA DE LOS DATOS

    print("\n=== MUESTRA DE LOS DATOS ===")
    df.show(5, truncate=False)



    # ESTADISTICAS DE CONTAMINANTES

    print("\n=== ESTADISTICAS DE CONTAMINANTES ===")
    contaminants = [
        "carbon_monoxide",
        "nitrogen_dioxide",
        "ozone",
        "particullate_matter",
        "sulfure_dioxide"
    ]
    df.select(contaminants).describe().show()

    # VALIDACION DE COORDENADAS

    print("\n=== VALIDACION DE COORDENADAS ===")
    df.select(
        count(
            when(
                (col("latitude") < -90) | (col("latitude") > 90),
                1
            )
        ).alias("latitudes_invalidas"),

        count(
            when(
                (col("longitude") < -180) | (col("longitude") > 180),
                1
            )
        ).alias("longitudes_invalidas")
    ).show()

    # VALIDACION DE FECHAS

    print("\n=== VALIDACION DE FECHAS ===")
    df.select(
        count(
            when(
                to_timestamp(
                    col("timestamp"),
                    "yyyy-MM-dd HH:mm:ss"
                ).isNull(),
                1
            )
        ).alias("fechas_invalidas")
    ).show()

    # VALIDACION DE DUPLICADOS

    print("\n=== VALIDACION DE DUPLICADOS ===")
    columnas_ubicacion = [
        "longitude",
        "latitude",
        "timestamp"
    ]
    registros_unicos = df.dropDuplicates(
        columnas_ubicacion
    ).count()
    duplicados = total_registros - registros_unicos
    print("Total de registros:", total_registros)
    print("Registros unicos:", registros_unicos)
    print("Registros duplicados:", duplicados)

    # ANALISIS DE POSIBLES DUPLICADOS

    print("\n=== ANALISIS DE POSIBLES DUPLICADOS ===")
    duplicados_detalle = (
        df.groupBy(*columnas_ubicacion)
        .agg(count("*").alias("cantidad"))
        .filter(col("cantidad") > 1)
    )
    cantidad_grupos = duplicados_detalle.count()
    print("Grupos con coincidencias:", cantidad_grupos)
    print("Grupos con mayor cantidad de repeticiones:")
    duplicados_detalle.orderBy(
        col("cantidad").desc()
    ).show(10, truncate=False)

    # COMPARACION DE VALORES DE CONTAMINACION

    print("\n=== COMPARACION DE VALORES DE CONTAMINACION ===")
    grupo_ejemplo = (
        duplicados_detalle
        .orderBy(col("cantidad").desc())
        .first()
    )
    if grupo_ejemplo:

        ejemplo = df.filter(
            (col("longitude") == grupo_ejemplo["longitude"]) &
            (col("latitude") == grupo_ejemplo["latitude"]) &
            (col("timestamp") == grupo_ejemplo["timestamp"])
        )

        ejemplo.select(
            "longitude",
            "latitude",
            "timestamp",
            "ozone",
            "particullate_matter",
            "carbon_monoxide",
            "sulfure_dioxide",
            "nitrogen_dioxide"
        ).show(truncate=False)

    # VALIDACION DE DUPLICADOS EXACTOS

    print("\n=== VALIDACION DE DUPLICADOS EXACTOS ===")
    columnas_datos = [
        "longitude",
        "latitude",
        "timestamp",
        "ozone",
        "particullate_matter",
        "carbon_monoxide",
        "sulfure_dioxide",
        "nitrogen_dioxide"
    ]
    registros_unicos_exactos = (
        df.dropDuplicates(columnas_datos).count()
    )
    duplicados_exactos = (
        total_registros - registros_unicos_exactos
    )
    print("Total de registros:", total_registros)
    print("Registros unicos:", registros_unicos_exactos)
    print("Duplicados exactos:", duplicados_exactos)

    # GRUPOS DE DUPLICADOS EXACTOS

    print("\n=== GRUPOS DE DUPLICADOS EXACTOS ===")
    grupos_duplicados = (
        df.groupBy(*columnas_datos)
        .agg(count("*").alias("cantidad"))
        .filter(col("cantidad") > 1)
    )
    print(
        "Cantidad de grupos duplicados:",
        grupos_duplicados.count()
    )
    print("Grupos con mayor cantidad de repeticiones:")
    grupos_duplicados.orderBy(
        col("cantidad").desc()
    ).show(10, truncate=False)

    # PREPARACION DE DATOS LIMPIOS

    print("\n=== PREPARACION DE DATOS LIMPIOS ===")
    df_limpio = df.dropDuplicates(columnas_datos)
    # Excluir el identificador original de MongoDB
    df_limpio = df_limpio.drop("_id")
    registros_limpios = df_limpio.count()
    duplicados_eliminados = (
    total_registros - registros_unicos_exactos
    )
    print("Documentos originales:", total_registros)
    print(
        "Documentos despues de eliminar duplicados:",
        registros_limpios
    )
    print(
        "Duplicados exactos eliminados:",
        duplicados_eliminados
    )

    # AGREGAR METADATOS

    print("\n=== AGREGANDO METADATOS ===")
    df_limpio = (
        df_limpio
        .withColumn("source_id", lit("source_001"))
        .withColumn(
            "record_id",
            sha2(
                to_json(
                    struct(*[
                        col(c) for c in columnas_datos
                    ])
                ),
                256
            )
        )
        .withColumn("quality_status", lit("valid"))
        .withColumn("processed_at", current_timestamp())
    )

    # VERIFICAR DATOS PREPARADOS

    print("\n=== DATOS PREPARADOS PARA MONGODB ===")
    df_limpio.printSchema()
    df_limpio.show(5, truncate=False)

    # VERIFICAR IDENTIFICADORES

    print("\n=== VERIFICANDO IDENTIFICADORES ===")
    registros_sin_id = (
        df_limpio
        .filter(col("record_id").isNull())
        .count()
    )
    print("Registros sin record_id:", registros_sin_id)
    if registros_sin_id > 0:
        raise ValueError(
            "Hay registros sin record_id. Se cancela el procesamiento."
        )

    # PROTEGER LA COLECCION CLEANED_DATA

    print("\n=== VERIFICANDO ESCRITURA EN CLEANED_DATA ===")
    print("Los datos ya fueron procesados anteriormente.")
    print("No se realizará una nueva escritura en cleaned_data.")
    registros_escritos = 0

    # El procesamiento y las validaciones terminaron
    estado_proceso = "completed"
    print("\n=== PROCESAMIENTO COMPLETADO ===")

except Exception as e:
    estado_proceso = "failed"
    print("\nERROR DURANTE EL PROCESAMIENTO:")
    print(str(e))

finally:

    # REGISTRAR EJECUCION EN PROCESSING_LOGS

    if spark is not None:
        try:
            print("\n=== PREPARANDO REGISTRO DE PROCESAMIENTO ===")
            log_data = [(
                "data_cleaning",
                "source_001",
                estado_proceso,
                total_registros,
                registros_escritos,
                duplicados_eliminados
            )]
            log_columns = [
                "process",
                "source_id",
                "status",
                "records_read",
                "records_written",
                "duplicates_removed"
            ]
            df_log = spark.createDataFrame(
                log_data,
                log_columns
            )
            df_log = df_log.withColumn(
                "executed_at",
                current_timestamp()
            )
            df_log.show(truncate=False)

            # Guardar registro en MongoDB
            print("\n=== GUARDANDO REGISTRO EN PROCESSING_LOGS ===")
            df_log.write \
                .format("mongodb") \
                .mode("append") \
                .option(
                    "connection.uri",
                    "mongodb://host.docker.internal:27017/smartcity.processing_logs"
                ) \
                .save()
            print(
                "Registro de procesamiento guardado correctamente."
            )
        except Exception as log_error:
            print("\nERROR AL GUARDAR EL REGISTRO DE PROCESAMIENTO:")
            print(str(log_error))

        finally:

            spark.stop()
            print("\n=== SPARK FINALIZADO ===")
