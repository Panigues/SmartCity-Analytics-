import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    date_format,
    dayofmonth,
    expr,
    greatest,
    isnan,
    lit,
    max as spark_max,
    min as spark_min,
    percentile_approx,
    round as spark_round,
    sqrt,
    sum as spark_sum,
    to_date,
    to_timestamp,
    when
)
from pyspark.sql.types import DecimalType


# ============================================================
# CONFIGURACION
# ============================================================

JDBC_DRIVER = "org.postgresql.Driver"
JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"

TABLA_DIARIA = "lectura_diaria"
TABLA_MENSUAL = "indicador_zona_mensual"
TABLA_ANOMALIAS = "anomalia_detectada"

STG_DIARIA = "stg_spark_lectura_diaria"
STG_MENSUAL = "stg_spark_indicador_zona_mensual"
STG_ANOMALIAS = "stg_spark_anomalia_detectada"

UMBRAL_ANOMALIA = 6.0

RANGOS = {
    "humedad": (0, 100),
    "temperatura": (-40, 50),
    "nivel_db": (0, 140),
    "velocidad_promedio": (0, 200),
    "ocupacion_pct": (0, 100),
}


def crear_argumentos():
    parser = argparse.ArgumentParser(
        description="Procesa mediciones IoT de HDFS y carga resultados en PostgreSQL."
    )

    parser.add_argument(
        "--entrada",
        default="hdfs://namenode:9000/smartcity/iot/processed"
    )
    parser.add_argument(
        "--anomalias",
        default="/opt/spark/work-dir/data/generated/anomalias_inyectadas.csv"
    )
    parser.add_argument(
        "--jdbc",
        default="jdbc:postgresql://postgres:5432/smartcity"
    )
    parser.add_argument(
        "--usuario",
        default=os.getenv("PG_USER", "smartcity")
    )
    parser.add_argument(
        "--password",
        default=os.getenv("PG_PASSWORD")
    )
    parser.add_argument(
        "--salida-hdfs",
        default="hdfs://namenode:9000/smartcity/iot/results/spark_a_postgres"
    )
    parser.add_argument(
        "--umbral",
        type=float,
        default=UMBRAL_ANOMALIA
    )

    return parser.parse_args()


def validar_columnas(df, requeridas, nombre):
    faltantes = [c for c in requeridas if c not in df.columns]

    if faltantes:
        raise RuntimeError(
            f"El conjunto {nombre} no contiene las columnas requeridas: {faltantes}. "
            f"Columnas encontradas: {df.columns}"
        )


def obtener_conexion(spark, jdbc, usuario, password):
    spark._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass(JDBC_DRIVER)
    return spark._jvm.java.sql.DriverManager.getConnection(
        jdbc, usuario, password
    )


def preparar_tablas_staging(spark, args):
    conexion = obtener_conexion(
        spark, args.jdbc, args.usuario, args.password
    )

    try:
        conexion.setAutoCommit(False)
        sentencia = conexion.createStatement()

        instrucciones = [
            f"DROP TABLE IF EXISTS {STG_DIARIA}",
            f"CREATE TABLE {STG_DIARIA} (LIKE {TABLA_DIARIA} INCLUDING DEFAULTS)",

            f"DROP TABLE IF EXISTS {STG_MENSUAL}",
            f"CREATE TABLE {STG_MENSUAL} (LIKE {TABLA_MENSUAL} INCLUDING DEFAULTS)",

            f"DROP TABLE IF EXISTS {STG_ANOMALIAS}",
            f"CREATE TABLE {STG_ANOMALIAS} (LIKE {TABLA_ANOMALIAS} INCLUDING DEFAULTS)",
        ]

        for sql in instrucciones:
            sentencia.execute(sql)

        sentencia.close()
        conexion.commit()

    finally:
        conexion.close()


def escribir_staging(df, tabla, args):
    propiedades = {
        "user": args.usuario,
        "password": args.password,
        "driver": JDBC_DRIVER,
        "batchsize": "5000"
    }

    (
        df.coalesce(4)
        .write
        .mode("append")
        .jdbc(
            url=args.jdbc,
            table=tabla,
            properties=propiedades
        )
    )


def contar_tabla(spark, args, tabla):
    return (
        spark.read
        .format("jdbc")
        .option("url", args.jdbc)
        .option("dbtable", tabla)
        .option("user", args.usuario)
        .option("password", args.password)
        .option("driver", JDBC_DRIVER)
        .load()
        .count()
    )


def publicar_resultados(spark, args):
    conexion = obtener_conexion(
        spark, args.jdbc, args.usuario, args.password
    )

    try:
        conexion.setAutoCommit(False)
        sentencia = conexion.createStatement()

        operaciones = [
            f"DELETE FROM {TABLA_DIARIA}",
            f"""
            INSERT INTO {TABLA_DIARIA}
                (id_sensor, fecha, variable, promedio, minimo, maximo, cantidad)
            SELECT
                id_sensor, fecha, variable, promedio, minimo, maximo, cantidad
            FROM {STG_DIARIA}
            """,

            f"DELETE FROM {TABLA_MENSUAL}",
            f"""
            INSERT INTO {TABLA_MENSUAL}
                (id_zona, anio, mes, variable, promedio, maximo, lecturas, anomalias)
            SELECT
                id_zona, anio, mes, variable, promedio, maximo, lecturas, anomalias
            FROM {STG_MENSUAL}
            """,

            f"DELETE FROM {TABLA_ANOMALIAS}",
            f"""
            INSERT INTO {TABLA_ANOMALIAS}
                (id_sensor, fecha_hora, variable, valor, esperado, puntaje)
            SELECT
                id_sensor, fecha_hora, variable, valor, esperado, puntaje
            FROM {STG_ANOMALIAS}
            """
        ]

        for sql in operaciones:
            sentencia.executeUpdate(sql)

        sentencia.close()
        conexion.commit()

    except Exception:
        conexion.rollback()
        raise

    finally:
        conexion.close()


def main():
    args = crear_argumentos()
    if not args.password:
        raise RuntimeError("Falta configurar PG_PASSWORD.")

    spark = (
        SparkSession.builder
        .appName("SmartCity-IoT-HDFS-a-PostgreSQL")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n========== ETL IOT: HDFS -> SPARK -> POSTGRESQL ==========")
        print(f"Entrada HDFS: {args.entrada}")
        print(f"PostgreSQL: {args.jdbc}")
        print(f"Umbral de anomalía: {args.umbral}")

        # ----------------------------------------------------
        # 1. LEER PARQUET DE HDFS
        # ----------------------------------------------------

        print("\n[1/9] Leyendo mediciones Parquet desde HDFS...")

        origen = spark.read.parquet(args.entrada)

        validar_columnas(
            origen,
            ["sensor_id", "id_zona", "timestamp", "variable", "valor"],
            "Parquet IoT"
        )

        print("Columnas de entrada verificadas.")

        datos = (
            origen
            .select(
                col("sensor_id").cast("string").alias("sensor_id"),
                col("id_zona").cast("string").alias("id_zona"),
                col("timestamp").cast("timestamp").alias("fecha_hora"),
                col("variable").cast("string").alias("variable"),
                col("valor").cast("double").alias("valor")
            )
        )

        # ----------------------------------------------------
        # 2. VALIDAR VALORES
        # ----------------------------------------------------

        print("\n[2/9] Validando valores y rangos físicos...")

        condicion = (
            col("sensor_id").isNotNull()
            & col("id_zona").isNotNull()
            & col("fecha_hora").isNotNull()
            & col("variable").isNotNull()
            & col("valor").isNotNull()
            & (~isnan(col("valor")))
            & (spark_abs(col("valor")) < lit(1.0e308))
        )

        # Temperatura admite valores negativos.
        # Las demás variables sin rango específico deben ser >= 0.
        for variable, limites in RANGOS.items():
            minimo, maximo = limites
            condicion = condicion & (
                (col("variable") != variable)
                | col("valor").between(minimo, maximo)
            )

        variables_con_rango = list(RANGOS.keys())

        condicion = condicion & (
            col("variable").isin(variables_con_rango)
            | (col("valor") >= 0)
        )

        validos = datos.filter(condicion).cache()

        total_origen = datos.count()
        total_validos = validos.count()
        rechazados = total_origen - total_validos

        print(f"Registros de entrada: {total_origen}")
        print(f"Registros válidos: {total_validos}")
        print(f"Registros rechazados: {rechazados}")

        if total_validos == 0:
            raise RuntimeError("No hay mediciones válidas para procesar.")

        # ----------------------------------------------------
        # 3. RELACIONAR SENSORES CON POSTGRESQL
        # ----------------------------------------------------

        print("\n[3/9] Relacionando sensores con el catálogo PostgreSQL...")

        propiedades = {
            "user": args.usuario,
            "password": args.password,
            "driver": JDBC_DRIVER
        }

        sensores = (
            spark.read
            .format("jdbc")
            .option("url", args.jdbc)
            .option(
                "dbtable",
                "(SELECT id_sensor, nombre AS sensor_id FROM sensor) AS sensores"
            )
            .options(**propiedades)
            .load()
        )

        sensores = sensores.select(
            col("sensor_id"),
            col("id_sensor").cast("int").alias("id_sensor_pg")
        )

        sin_sensor = (
            validos.select("sensor_id")
            .distinct()
            .join(sensores.select("sensor_id"), "sensor_id", "left_anti")
            .limit(1)
            .count()
        )

        if sin_sensor > 0:
            raise RuntimeError(
                "Hay sensores de las mediciones que no existen en PostgreSQL."
            )

        datos_sensor = (
            validos
            .join(sensores, "sensor_id", "inner")
            .withColumn("fecha", to_date("fecha_hora"))
            .withColumn("anio", date_format("fecha_hora", "yyyy").cast("int"))
            .withColumn("mes", date_format("fecha_hora", "MM").cast("int"))
            .withColumn("hora", date_format("fecha_hora", "HH").cast("int"))
            .cache()
        )

        # ----------------------------------------------------
        # 4. DETECCION DE ANOMALIAS MEDIANTE MEDIANA Y MAD
        # ----------------------------------------------------

        print("\n[4/9] Calculando medianas y desviación absoluta mediana...")

        claves = ["sensor_id", "variable", "hora"]

        medianas = (
            datos_sensor
            .groupBy(*claves)
            .agg(
                percentile_approx("valor", 0.5, 10000).alias("mediana")
            )
        )

        desviaciones = (
            datos_sensor
            .join(medianas, claves, "inner")
            .withColumn(
                "desviacion_absoluta",
                spark_abs(col("valor") - col("mediana"))
            )
            .groupBy(*claves)
            .agg(
                percentile_approx(
                    "desviacion_absoluta", 0.5, 10000
                ).alias("mad")
            )
        )

        puntuaciones = (
            datos_sensor
            .join(medianas, claves, "inner")
            .join(desviaciones, claves, "inner")
            .withColumn(
                "escala",
                greatest(
                    col("mad") * lit(1.4826),
                    spark_abs(col("mediana")) * lit(0.05),
                    lit(0.001)
                )
            )
            .withColumn(
                "puntaje_calculado",
                (col("valor") - col("mediana")) / col("escala")
            )
            .withColumn(
                "es_anomalia",
                spark_abs(col("puntaje_calculado")) > lit(args.umbral)
            )
            .cache()
        )

        total_anomalias = (
            puntuaciones
            .filter(col("es_anomalia"))
            .count()
        )

        print(f"Anomalías detectadas por Spark: {total_anomalias}")

        # ----------------------------------------------------
        # 5. COMPARAR CON ANOMALIAS INYECTADAS
        # ----------------------------------------------------

        print("\n[5/9] Comparando con las anomalías inyectadas...")

        inyectadas = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(args.anomalias)
            .select(
                col("sensor").cast("string").alias("sensor_id"),
                to_timestamp(
                    col("timestamp"),
                    "yyyy-MM-dd HH:mm:ss"
                ).alias("fecha_hora"),
                col("variable").cast("string").alias("variable")
            )
            .dropDuplicates(["sensor_id", "fecha_hora", "variable"])
        )

        total_inyectadas = inyectadas.count()

        detectadas_inyectadas = (
            inyectadas
            .join(
                puntuaciones
                .filter(col("es_anomalia"))
                .select("sensor_id", "fecha_hora", "variable")
                .dropDuplicates(),
                ["sensor_id", "fecha_hora", "variable"],
                "inner"
            )
            .count()
        )

        recall = (
            detectadas_inyectadas / total_inyectadas
            if total_inyectadas > 0 else 0.0
        )

        print(f"Anomalías inyectadas: {total_inyectadas}")
        print(f"Anomalías inyectadas detectadas: {detectadas_inyectadas}")
        print(f"Recall de anomalías inyectadas: {recall:.2%}")

        # ----------------------------------------------------
        # 6. CONSTRUIR RESULTADOS
        # ----------------------------------------------------

        print("\n[6/9] Construyendo agregaciones diarias y mensuales...")

        normales = puntuaciones.filter(~col("es_anomalia"))

        diaria = (
            normales
            .groupBy("id_sensor_pg", "fecha", "variable")
            .agg(
                spark_round(avg("valor"), 4).cast(DecimalType(12, 4)).alias("promedio"),
                spark_round(spark_min("valor"), 4).cast(DecimalType(12, 4)).alias("minimo"),
                spark_round(spark_max("valor"), 4).cast(DecimalType(12, 4)).alias("maximo"),
                count("valor").cast("int").alias("cantidad")
            )
            .withColumnRenamed("id_sensor_pg", "id_sensor")
            .select(
                "id_sensor",
                "fecha",
                "variable",
                "promedio",
                "minimo",
                "maximo",
                "cantidad"
            )
        )

        maximo_diario = diaria.agg(spark_max("cantidad")).first()[0]

        if maximo_diario is not None and maximo_diario > 24:
            raise RuntimeError(
                f"Una agregación diaria contiene {maximo_diario} lecturas; "
                "la tabla permite un máximo de 24."
            )

        mensual = (
            puntuaciones
            .groupBy("id_zona", "anio", "mes", "variable")
            .agg(
                spark_round(
                    avg(when(~col("es_anomalia"), col("valor"))), 4
                ).cast(DecimalType(12, 4)).alias("promedio"),

                spark_round(
                    spark_max(when(~col("es_anomalia"), col("valor"))), 4
                ).cast(DecimalType(12, 4)).alias("maximo"),

                count("valor").cast("int").alias("lecturas"),

                spark_sum(
                    when(col("es_anomalia"), 1).otherwise(0)
                ).cast("int").alias("anomalias")
            )
        )

        if mensual.filter(
            col("promedio").isNull() | col("maximo").isNull()
        ).limit(1).count() > 0:
            raise RuntimeError(
                "Hay grupos mensuales sin valores normales para calcular "
                "promedio y máximo."
            )

        detalle_anomalias = (
            puntuaciones
            .filter(col("es_anomalia"))
            .select(
                col("id_sensor_pg").alias("id_sensor"),
                col("fecha_hora"),
                col("variable"),
                spark_round("valor", 4).cast(DecimalType(12, 4)).alias("valor"),
                spark_round("mediana", 4).cast(DecimalType(12, 4)).alias("esperado"),
                spark_round(
                    spark_abs("puntaje_calculado"), 2
                ).cast(DecimalType(10, 2)).alias("puntaje")
            )
        )

        # ----------------------------------------------------
        # 7. PREPARAR Y ESCRIBIR TABLAS STAGING
        # ----------------------------------------------------

        print("\n[7/9] Preparando tablas temporales de PostgreSQL...")

        preparar_tablas_staging(spark, args)

        print("Escribiendo agregaciones diarias...")
        escribir_staging(diaria, STG_DIARIA, args)

        print("Escribiendo indicadores mensuales...")
        escribir_staging(mensual, STG_MENSUAL, args)

        print("Escribiendo detalle de anomalías...")
        escribir_staging(detalle_anomalias, STG_ANOMALIAS, args)

        # ----------------------------------------------------
        # 8. VALIDAR STAGING Y GUARDAR RESULTADOS EN HDFS
        # ----------------------------------------------------

        print("\n[8/9] Validando registros preparados...")

        cantidad_diaria = contar_tabla(spark, args, STG_DIARIA)
        cantidad_mensual = contar_tabla(spark, args, STG_MENSUAL)
        cantidad_detalle = contar_tabla(spark, args, STG_ANOMALIAS)

        print(f"Staging lectura_diaria: {cantidad_diaria}")
        print(f"Staging indicador_zona_mensual: {cantidad_mensual}")
        print(f"Staging anomalia_detectada: {cantidad_detalle}")

        if cantidad_diaria != diaria.count():
            raise RuntimeError("La cantidad de filas diarias no coincide.")

        if cantidad_mensual != mensual.count():
            raise RuntimeError("La cantidad de filas mensuales no coincide.")

        if cantidad_detalle != detalle_anomalias.count():
            raise RuntimeError("La cantidad de anomalías no coincide.")

        # Guardar una copia de los indicadores calculados en HDFS.
        # La ruta debe ser nueva; no se sobrescriben resultados anteriores.
        print("Guardando indicadores mensuales en HDFS...")

        (
            mensual.write
            .mode("errorifexists")
            .parquet(args.salida_hdfs)
        )

        # ----------------------------------------------------
        # 9. PUBLICAR RESULTADOS DEFINITIVOS EN UNA TRANSACCION
        # ----------------------------------------------------

        print("\n[9/9] Publicando resultados en PostgreSQL...")

        publicar_resultados(spark, args)

        print("\n========== ETL COMPLETADO CORRECTAMENTE ==========")
        print(f"Registros de entrada: {total_origen}")
        print(f"Registros válidos: {total_validos}")
        print(f"Registros rechazados: {rechazados}")
        print(f"Anomalías detectadas: {total_anomalias}")
        print(f"Anomalías inyectadas: {total_inyectadas}")
        print(f"Anomalías inyectadas detectadas: {detectadas_inyectadas}")
        print(f"Recall: {recall:.2%}")
        print(f"Lecturas diarias cargadas: {cantidad_diaria}")
        print(f"Indicadores mensuales cargados: {cantidad_mensual}")
        print(f"Anomalías cargadas: {cantidad_detalle}")
        print(f"Indicadores HDFS: {args.salida_hdfs}")
        print("Las tres tablas definitivas se actualizaron en una transacción.")

        puntuaciones.unpersist()
        datos_sensor.unpersist()
        validos.unpersist()

    except Exception as error:
        print("\n========== ERROR EN EL ETL ==========")
        print(str(error))
        print(
            "Los resultados definitivos no se publican si ocurre un error "
            "antes de completar la transacción."
        )
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
