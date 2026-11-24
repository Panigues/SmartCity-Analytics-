# Q1 - Mapper: una medición (fila del formato largo) -> "sensor<TAB>1"
# Columnas: timestamp,zone_id,domain,tipo,sensor_id,variable,value,unit
BEGIN { FS = "," }
$1 == "timestamp" { next }          # encabezado del CSV
{ print $5 "\t1" }
