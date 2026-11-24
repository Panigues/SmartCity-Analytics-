# Q4 - Mapper: un "evento" es una medición que supera su límite de alerta
# (límites de config/smartcity_config.json). Emite "zona|variable<TAB>1"
BEGIN {
  FS = ","
  lim["pm25"] = 35; lim["pm10"] = 50; lim["no2"] = 200; lim["co"] = 10
  lim["co2"] = 1000; lim["nivel_db"] = 70; lim["ocupacion_pct"] = 95
}
$1 == "timestamp" { next }
($6 in lim) && ($7 + 0 > lim[$6]) { print $2 "|" $6 "\t1"; next }
$6 == "velocidad_promedio" && $7 + 0 < 15 { print $2 "|velocidad_baja\t1" }
