# Reducer: promedio = suma total / conteo total -> "clave<TAB>promedio<TAB>conteo"
BEGIN { FS = OFS = "\t" }
{ s[$1] += $2; n[$1] += $3 }
END { for (k in s) printf "%s\t%.2f\t%d\n", k, s[k] / n[k], n[k] }
