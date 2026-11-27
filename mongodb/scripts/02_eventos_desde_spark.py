"""
Eventos detectados -> MongoDB

Lleva a la colección `eventos` de MongoDB las anomalías que Spark detectó
(tabla anomalia_detectada de PostgreSQL), cada una con su contexto: la
lectura completa del sensor en ese momento, tomada de iot_readings.

Integración: Spark -> PostgreSQL -> MongoDB.

Uso (desde la raíz del repo, después de 01_modelo_documental.js y del ETL de Spark):
    python mongodb/scripts/02_eventos_desde_spark.py

Variables opcionales: PG_HOST (localhost), PG_PORT (5433), MONGO_URI (mongodb://localhost:27017)
"""
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from pymongo import InsertOne, MongoClient

RAIZ = Path(__file__).resolve().parents[2]


def leer_env():
    valores = {}
    ruta = RAIZ / ".env"
    if ruta.exists():
        for linea in ruta.read_text(encoding="utf-8").splitlines():
            if "=" in linea and not linea.strip().startswith("#"):
                k, v = linea.split("=", 1)
                valores[k.strip()] = v.strip()
    valores.update(os.environ)
    return valores


def severidad(puntaje):
    p = abs(puntaje)
    return "critica" if p >= 30 else "alta" if p >= 12 else "media"


def main():
    env = leer_env()
    pg = psycopg2.connect(
        host=env.get("PG_HOST", "localhost"), port=env.get("PG_PORT", "5433"),
        dbname=env.get("POSTGRES_DB", "smartcity"),
        user=env.get("POSTGRES_USER", env.get("PG_USER", "smartcity")),
        password=env.get("POSTGRES_PASSWORD", env.get("PG_PASSWORD", "")))
    db = MongoClient(env.get("MONGO_URI", "mongodb://localhost:27017"))[env.get("MONGO_DB", "smartcity")]

    with pg, pg.cursor() as cur:
        cur.execute("""
            SELECT a.fecha_hora, d.sensor, d.tipo, d.categoria, d.id_zona, d.zona, d.organismo,
                   a.variable, a.valor::float, a.esperado::float, a.puntaje::float,
                   d.latitud::float, d.longitud::float
            FROM anomalia_detectada a
            JOIN v_sensor_detalle d ON d.id_sensor = a.id_sensor""")
        filas = cur.fetchall()
    pg.close()
    print(f"Anomalías leídas desde PostgreSQL (resultado de Spark): {len(filas):,}")

    db.eventos.delete_many({"origen": "Spark"})
    lecturas = db.iot_readings
    ops = []
    for (fh, sensor, tipo, categoria, id_zona, zona, organismo, variable,
         valor, esperado, puntaje, lat, lon) in filas:
        ts = fh.replace(tzinfo=timezone.utc) if fh.tzinfo is None else fh
        contexto = lecturas.find_one({"sensor_id": sensor, "timestamp": ts},
                                     {"_id": 0, "mediciones": 1})
        doc = {
            "_id": f"{sensor}|{ts.isoformat()}|{variable}",
            "tipo_evento": "anomalia_estadistica",
            "origen": "Spark",
            "metodo": "desviación robusta (mediana y MAD por sensor, variable y hora del día)",
            "sensor_id": sensor,
            "tipo_sensor": tipo,
            "categoria": categoria,
            "organismo_responsable": organismo,
            "zona": {"id": id_zona, "nombre": zona},
            "ubicacion": {"type": "Point", "coordinates": [lon, lat]},
            "timestamp": ts,
            "variable": variable,
            "valor": valor,
            "esperado": esperado,
            "desviacion": round(valor - esperado, 3),
            "puntaje": puntaje,
            "severidad": severidad(puntaje),
            "direccion": "por_encima" if valor > esperado else "por_debajo",
            "registrado_en": datetime.now(timezone.utc),
        }
        if contexto:                      # lectura completa del sensor en ese instante
            doc["lectura_completa"] = contexto["mediciones"]
        ops.append(InsertOne(doc))
        if len(ops) == 5000:
            db.eventos.bulk_write(ops, ordered=False); ops = []
    if ops:
        db.eventos.bulk_write(ops, ordered=False)

    print(f"Eventos guardados en MongoDB: {db.eventos.count_documents({'origen': 'Spark'}):,}")
    for r in db.eventos.aggregate([{"$group": {"_id": "$severidad", "n": {"$sum": 1}}}, {"$sort": {"n": -1}}]):
        print(f"   {r['_id']:<8} {r['n']:,}")


if __name__ == "__main__":
    main()
