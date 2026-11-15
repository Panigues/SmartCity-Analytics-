# Combiner: pre-suma (suma, conteo) en cada nodo antes de enviar por la red
BEGIN { FS = OFS = "\t" }
{ s[$1] += $2; n[$1] += $3 }
END { for (k in s) print k, s[k], n[k] }
