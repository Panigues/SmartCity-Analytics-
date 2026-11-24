# Reducer / Combiner: suma los valores de cada clave -> "clave<TAB>total"
BEGIN { FS = OFS = "\t" }
{ total[$1] += $2 }
END { for (k in total) print k, total[k] }
