# Q2 - Reducer: conserva el sensor con más mediciones (y los empates)
BEGIN { FS = OFS = "\t"; mejor = -1 }
{
  if ($3 + 0 > mejor) { mejor = $3 + 0; lista = $2 }
  else if ($3 + 0 == mejor) { lista = lista "," $2 }
}
END { print "sensor_con_mas_datos", lista, mejor }
