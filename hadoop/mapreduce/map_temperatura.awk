# Q3 - Mapper: temperatura por zona y total de la ciudad
# Emite "clave<TAB>suma<TAB>conteo" para poder usar un combiner
BEGIN { FS = "," }
$6 == "temperatura" {
  print $2 "\t" $7 "\t1"
  print "CIUDAD\t" $7 "\t1"
}
