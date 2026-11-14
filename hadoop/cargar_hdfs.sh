#!/bin/bash
# =====================================================================
# SmartCity Analytics - Estructura y carga de datos en HDFS
#
# Se ejecuta DENTRO del contenedor namenode, que ve la carpeta data/
# del repo en /data (volumen definido en docker-compose.yml).
#
# Uso (PowerShell, desde la raíz del repo):
#   docker exec -it namenode bash -c "tr -d '\r' < /scripts/cargar_hdfs.sh | bash"
#
# (el "tr -d '\r'" quita los saltos de línea de Windows, por si el
#  archivo se guardó con CRLF)
# =====================================================================
set -e

BASE=/smartcity
LOCAL=/data

echo "== 1. Estructura de directorios =================================="
# raw/       datos originales, sin modificar (fuente de verdad)
# lecturas/  lecturas IoT simuladas, particionadas por año y mes
#            (formato anio=AAAA/mes=MM que Spark y Hive reconocen solos)
# exports/   copias exportadas desde MongoDB (JSON) y PostgreSQL (CSV)
# eventos/   eventos históricos (anomalías registradas)
# results/   resultados de los procesamientos (MapReduce / Spark)
hdfs dfs -mkdir -p \
  $BASE/raw/energia/uci \
  $BASE/raw/movilidad/citypulse/traffic \
  $BASE/raw/ambiente/openaq \
  $BASE/lecturas \
  $BASE/exports/json \
  $BASE/exports/csv \
  $BASE/eventos \
  $BASE/results

echo "== 2. Datos crudos (raw) ========================================="
hdfs dfs -put -f $LOCAL/raw/uci/household_power_consumption.txt $BASE/raw/energia/uci/
hdfs dfs -put -f $LOCAL/raw/citypulse/trafficMetaData.csv        $BASE/raw/movilidad/citypulse/
echo "   tráfico CityPulse (449 archivos, puede tardar)..."
hdfs dfs -put -f $LOCAL/raw/citypulse/traffic/*.csv               $BASE/raw/movilidad/citypulse/traffic/
hdfs dfs -put -f $LOCAL/raw/openaq/openaq_*.csv                   $BASE/raw/ambiente/openaq/
for extra in parking weather; do
  if [ -d "$LOCAL/raw/citypulse/$extra" ]; then
    hdfs dfs -mkdir -p $BASE/raw/movilidad/citypulse/$extra
    hdfs dfs -put -f $LOCAL/raw/citypulse/$extra/* $BASE/raw/movilidad/citypulse/$extra/
  fi
done

echo "== 3. Lecturas IoT simuladas, particionadas por año y mes ========"
for archivo in $LOCAL/generated/hdfs/lecturas_*.csv; do
  nombre=$(basename "$archivo" .csv)       # lecturas_2024_01
  anio=$(echo "$nombre" | cut -d_ -f2)
  mes=$(echo "$nombre" | cut -d_ -f3)
  destino=$BASE/lecturas/anio=$anio/mes=$mes
  hdfs dfs -mkdir -p "$destino"
  hdfs dfs -put -f "$archivo" "$destino/lecturas.csv"
  echo "   $anio-$mes"
done

echo "== 4. Exportaciones y eventos ===================================="
hdfs dfs -put -f $LOCAL/generated/mongodb/lecturas_*.jsonl     $BASE/exports/json/
hdfs dfs -put -f $LOCAL/generated/postgres/lectura_diaria.csv  $BASE/exports/csv/
hdfs dfs -put -f $LOCAL/generated/anomalias_inyectadas.csv     $BASE/eventos/

echo "== 5. Resumen ===================================================="
hdfs dfs -du -s -h $BASE/raw $BASE/lecturas $BASE/exports $BASE/eventos
echo
hdfs dfs -count -h $BASE
echo
hdfs fsck $BASE | grep -E "Total size|Total files|Total blocks|Default replication|Average block replication|Under-replicated|Missing blocks|Status"
