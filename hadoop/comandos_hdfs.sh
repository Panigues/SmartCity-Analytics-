#!/bin/bash
# =====================================================================
# SmartCity Analytics - Demostración de operaciones HDFS
#
# Recorre las operaciones que pide el enunciado y muestra qué pasa
# cuando un archivo se distribuye entre los nodos:
#   mkdir, put, ls, cat, get, rm  +  stat, fsck (bloques y réplicas)
#
# Uso (PowerShell, desde la raíz del repo):
#   docker exec -it namenode bash -c "tr -d '\r' < /scripts/comandos_hdfs.sh | bash"
#
# Trabaja en /smartcity/demo y lo borra al final: no toca los datos del proyecto.
# =====================================================================
set -e

DEMO=/smartcity/demo
LOCAL=/tmp/demo_hdfs
GRANDE=/smartcity/raw/energia/uci/household_power_consumption.txt

paso() { echo; echo "=================================================================="; echo "$1"; echo "=================================================================="; }
cmd()  { echo "\$ $*"; "$@" 2>&1 | grep -v -E "INFO|WARN" || true; }

rm -rf $LOCAL && mkdir -p $LOCAL
hdfs dfs -rm -r -f -skipTrash $DEMO > /dev/null 2>&1 || true

paso "1. hdfs dfs -mkdir  -> crear directorios"
cmd hdfs dfs -mkdir -p $DEMO/entrada $DEMO/salida
cmd hdfs dfs -ls $DEMO

paso "2. hdfs dfs -put  -> subir un archivo local a HDFS"
# Muestra de 5 000 lecturas generadas por el simulador IoT
ORIGEN=$(ls /data/generated/hdfs/lecturas_*.csv 2>/dev/null | head -1)
if [ -n "$ORIGEN" ]; then head -n 5000 "$ORIGEN" > $LOCAL/muestra_lecturas.csv
else hdfs dfs -cat "/smartcity/lecturas/anio=2024/mes=01/lecturas.csv" 2>/dev/null | head -n 5000 > $LOCAL/muestra_lecturas.csv; fi
echo "Archivo local: $(wc -l < $LOCAL/muestra_lecturas.csv) líneas, $(du -h $LOCAL/muestra_lecturas.csv | cut -f1)"
cmd hdfs dfs -put -f $LOCAL/muestra_lecturas.csv $DEMO/entrada/

paso "3. hdfs dfs -ls  -> listar el contenido"
cmd hdfs dfs -ls -h $DEMO/entrada
echo "La segunda columna del listado es el factor de replicación del archivo."

paso "4. hdfs dfs -cat  -> leer el archivo desde HDFS"
echo "\$ hdfs dfs -cat $DEMO/entrada/muestra_lecturas.csv | head -n 6"
hdfs dfs -cat $DEMO/entrada/muestra_lecturas.csv 2>/dev/null | head -n 6

paso "5. hdfs dfs -get  -> descargar de HDFS al sistema local"
cmd hdfs dfs -get -f $DEMO/entrada/muestra_lecturas.csv $LOCAL/descargado.csv
echo "Comprobación de integridad (MD5 del original y del descargado):"
md5sum $LOCAL/muestra_lecturas.csv $LOCAL/descargado.csv

paso "6. ¿Qué pasa cuando un archivo se distribuye entre los nodos?"
echo "Archivo grande del proyecto: $GRANDE"
cmd hdfs dfs -stat "tamaño=%b bytes · tamaño de bloque=%o bytes · replicación=%r" $GRANDE
echo
echo "\$ hdfs fsck $GRANDE -files -blocks -locations"
hdfs fsck $GRANDE -files -blocks -locations 2>/dev/null | grep -E "bytes|blk_|Live_repl|Status" | head -20
echo
echo "Explicación:"
echo " - El NameNode divide el archivo en bloques de 32 MB (dfs.blocksize) y solo guarda"
echo "   el índice: qué bloques forman el archivo y en qué DataNodes está cada uno."
echo " - Cada bloque se copia en 2 DataNodes (dfs.replication=2): si un nodo falla,"
echo "   el bloque se sigue leyendo desde el otro (tolerancia a fallos)."
echo " - Al leer, el cliente pide al NameNode la ubicación de los bloques y los lee"
echo "   directamente de los DataNodes; los trabajos MapReduce/Spark se ejecutan donde"
echo "   están los bloques (procesamiento cercano a los datos)."
echo " - Para crecer basta con agregar DataNodes: la capacidad aumenta sin cambiar"
echo "   la aplicación (escalabilidad horizontal)."
echo
cmd hdfs dfsadmin -report
echo "(arriba: capacidad y estado de cada DataNode)"

paso "7. hdfs dfs -rm  -> eliminar archivos y directorios"
cmd hdfs dfs -rm -skipTrash $DEMO/entrada/muestra_lecturas.csv
cmd hdfs dfs -rm -r -skipTrash $DEMO
echo "\$ hdfs dfs -ls /smartcity"
hdfs dfs -ls /smartcity 2>/dev/null | grep -v -E "INFO|WARN"
echo
echo "Demostración terminada: el directorio $DEMO ya no existe."
rm -rf $LOCAL
