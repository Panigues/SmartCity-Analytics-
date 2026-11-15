#!/bin/bash
# =====================================================================
# SmartCity Analytics - Jobs MapReduce (Hadoop Streaming) sobre HDFS
#
# Responde las preguntas de Hadoop del enunciado:
#   q1  ¿Cuántas mediciones existen por sensor?
#   q2  ¿Cuál sensor produjo mayor cantidad de datos?
#   q3  ¿Cuál es la temperatura promedio histórica?
#   q4  ¿Cuál es la cantidad de eventos por zona?
#   q5  ¿Cuál es el comportamiento de los sensores durante un período?
#
# Uso (PowerShell, desde la raíz del repo):
#   docker exec -it namenode bash -c "tr -d '\r' < /scripts/ejecutar_mapreduce.sh > /tmp/mr.sh && bash /tmp/mr.sh todas"
#   (en lugar de "todas": q1, q2, q3, q4, o "q5 vehiculos 2025 01" = variable, año, mes)
#
# Los mappers y reducers están en hadoop/mapreduce/*.awk. Se usa awk porque
# viene instalado en los nodos de Hadoop (no requiere instalar Python).
# =====================================================================
set -e

JAR=/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar
LECTURAS="/smartcity/lecturas/anio=*/mes=*/lecturas.csv"
OUT=/smartcity/results
MR=/tmp/mapreduce

# Copia los .awk quitando saltos de línea de Windows (CRLF)
rm -rf $MR && mkdir -p $MR
for f in /scripts/mapreduce/*.awk; do tr -d '\r' < "$f" > "$MR/$(basename "$f")"; done

# streaming <nombre> <entrada> <salida> <mapper> <combiner|-> <reducer> <n_reducers> [extra...]
streaming() {
  local nombre=$1 entrada=$2 salida=$3 mapper=$4 combiner=$5 reducer=$6 nred=$7; shift 7
  local archivos="$MR/$mapper,$MR/$reducer"
  local opc_comb=()
  if [ "$combiner" != "-" ]; then
    archivos="$archivos,$MR/$combiner"
    opc_comb=(-combiner "awk -f $combiner")
  fi
  hdfs dfs -rm -r -f -skipTrash "$salida" > /dev/null
  echo ">> $nombre"
  inicio=$(date +%s)
  hadoop jar $JAR \
    -D mapreduce.job.name="$nombre" \
    -D mapreduce.job.reduces=$nred \
    -files "$archivos" \
    "$@" \
    -input "$entrada" \
    -output "$salida" \
    -mapper "awk -f $mapper" \
    "${opc_comb[@]}" \
    -reducer "awk -f $reducer" 2>&1 | grep -E "Running job|map [0-9]+% reduce|completed|ERROR|Launched map|Map input records|Combine output|Reduce input records" || true
  echo "   Tiempo: $(( $(date +%s) - inicio )) s"
}

q1() {
  streaming "Q1 mediciones por sensor" "$LECTURAS" $OUT/q1_mediciones_por_sensor \
    map_mediciones_sensor.awk red_sumar.awk red_sumar.awk 2
  echo "   Primeros sensores (de 168):"
  hdfs dfs -cat $OUT/q1_mediciones_por_sensor/part-* | sort | head -10
}

q2() {
  streaming "Q2 sensor con mas datos" "$OUT/q1_mediciones_por_sensor/part-*" $OUT/q2_sensor_mas_datos \
    map_maximo.awk - red_maximo.awk 1
  hdfs dfs -cat $OUT/q2_sensor_mas_datos/part-*
}

q3() {
  streaming "Q3 temperatura promedio" "$LECTURAS" $OUT/q3_temperatura_promedio \
    map_temperatura.awk comb_suma_conteo.awk red_promedio.awk 1
  echo "   clave | temperatura promedio (°C) | mediciones"
  hdfs dfs -cat $OUT/q3_temperatura_promedio/part-* | sort
}

q4() {
  streaming "Q4 eventos por zona" "$LECTURAS" $OUT/q4_eventos_por_zona \
    map_eventos_zona.awk red_sumar.awk red_sumar.awk 1
  echo "   zona|tipo de evento | cantidad"
  hdfs dfs -cat $OUT/q4_eventos_por_zona/part-* | sort
}

q5() {
  local var=${1:-vehiculos} anio=${2:-2025} mes=${3:-01}
  streaming "Q5 $var $anio-$mes" "/smartcity/lecturas/anio=$anio/mes=$mes/lecturas.csv" \
    $OUT/q5_periodo_${var}_${anio}_${mes} \
    map_periodo.awk comb_suma_conteo.awk red_promedio.awk 1 -cmdenv VARIABLE=$var
  echo "   fecha|zona | promedio diario de $var | mediciones"
  hdfs dfs -cat $OUT/q5_periodo_${var}_${anio}_${mes}/part-* | sort | head -20
}

case "${1:-todas}" in
  q1) q1 ;;  q2) q2 ;;  q3) q3 ;;  q4) q4 ;;
  q5) shift; q5 "$@" ;;
  todas) q1; q2; q3; q4; q5 ;;
  *) echo "Opción no válida: $1 (use q1..q5 o todas)"; exit 1 ;;
esac
