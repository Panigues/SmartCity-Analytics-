#!/bin/bash
# =====================================================================
# Importa las lecturas generadas (data/generated/mongodb/*.jsonl) a la
# colección iot_readings. Se ejecuta DENTRO del contenedor smartcity-mongo:
#
#   docker exec -it smartcity-mongo bash -c "tr -d '\r' < /scripts/importar_lecturas.sh | bash"
#
# Re-ejecutable: la primera importación usa --drop y reemplaza la colección.
# =====================================================================
set -e
ARCHIVOS=$(ls /datos/generated/mongodb/lecturas_*.jsonl 2>/dev/null || true)
if [ -z "$ARCHIVOS" ]; then
  echo "No hay archivos en data/generated/mongodb/. Ejecute primero: python generator/generar_lecturas.py"
  exit 1
fi
PRIMERO=1
for f in $ARCHIVOS; do
  if [ $PRIMERO -eq 1 ]; then OPC="--drop"; PRIMERO=0; else OPC=""; fi
  mongoimport --quiet --db smartcity --collection iot_readings --file "$f" --numInsertionWorkers 4 $OPC
  echo "  importado $(basename "$f")"
done
echo
echo "Documentos en iot_readings: $(mongosh --quiet smartcity --eval 'db.iot_readings.countDocuments()')"
