# Q2 - Mapper: lee la salida de Q1 ("sensor<TAB>total") y manda todo a una
# sola clave para que un único reducer compare todos los sensores
BEGIN { FS = "\t" }
{ print "max\t" $1 "\t" $2 }
