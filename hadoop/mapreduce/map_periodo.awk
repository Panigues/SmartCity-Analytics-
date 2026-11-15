# Q5 - Mapper: promedio diario por zona de una variable en un período.
# La variable llega como variable de entorno VARIABLE (-cmdenv VARIABLE=...)
# y el período se elige por las particiones de entrada (anio=/mes=).
BEGIN { FS = ","; var = ENVIRON["VARIABLE"] }
$6 == var { print substr($1, 1, 10) "|" $2 "\t" $7 "\t1" }
