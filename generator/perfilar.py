"""
Perfilador de datasets reales -> perfiles típicos para la simulación IoT.

Lee los datos crudos de data/raw/ y calcula, para cada dominio, el
comportamiento "típico" por día de la semana y hora del día:

    Energía   (UCI)        -> consumo de un hogar por hora y día
    Movilidad (CityPulse)  -> vehículos, velocidad y tiempo por zona Z01..Z05
    Ambiente  (OpenAQ)     -> PM10, NO2 y CO por hora y día

También asigna cada uno de los 449 sensores reales de tráfico de Aarhus a
una zona, usando las coordenadas de trafficMetaData.csv y las reglas
geográficas de config/smartcity_config.json.

Uso (desde la raíz del repo):
    python generator/perfilar.py

Salida: data/profiles/  (archivos pequeños, sí se suben a GitHub)
"""
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parent.parent
RAW = RAIZ / "data" / "raw"
SALIDA = RAIZ / "data" / "profiles"
CONFIG = json.loads((RAIZ / "config" / "smartcity_config.json").read_text(encoding="utf-8"))

DIAS = ["lun", "mar", "mie", "jue", "vie", "sab", "dom"]


# =============================================================== utilidades
def perfil_semanal(df, columnas, grupo=None):
    """Media y desviación estándar por (día de la semana, hora)."""
    claves = ([grupo] if grupo else []) + ["dia_semana", "hora"]
    agg = {}
    for c in columnas:
        agg[f"{c}_media"] = (c, "mean")
        agg[f"{c}_std"] = (c, "std")
    return df.groupby(claves).agg(**agg).round(4).reset_index()


def factor_mensual(df, columna, grupo=None):
    """Factor estacional: media del mes / media anual (1.0 = mes promedio)."""
    claves = ([grupo] if grupo else []) + ["mes"]
    mensual = df.groupby(claves)[columna].mean()
    if grupo:
        anual = df.groupby(grupo)[columna].mean()
        factor = mensual / mensual.index.get_level_values(0).map(anual).values
    else:
        factor = mensual / df[columna].mean()
    return factor.round(4).rename("factor").reset_index()


def distancia_y_rumbo(lat1, lon1, lat2, lon2):
    """Distancia (km) y rumbo (grados, 0 = norte, 90 = este) desde el punto 1."""
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dlat, dlon = p2 - p1, np.radians(lon2 - lon1)
    a = np.sin(dlat / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlon / 2) ** 2
    dist = 2 * 6371 * np.arcsin(np.sqrt(a))
    x = np.sin(dlon) * np.cos(p2)
    y = np.cos(p1) * np.sin(p2) - np.sin(p1) * np.cos(p2) * np.cos(dlon)
    rumbo = (np.degrees(np.arctan2(x, y)) + 360) % 360
    return dist, rumbo


def asignar_zona(dist_km, rumbo):
    """Aplica las reglas 'asignacion_geo' del JSON (radio para el centro, sectores para el resto)."""
    for z in CONFIG["zonas"]:
        g = z["asignacion_geo"]
        if g["tipo"] == "radio" and dist_km <= g["radio_km"]:
            return z["zone_id"]
    for z in CONFIG["zonas"]:
        g = z["asignacion_geo"]
        if g["tipo"] == "sector":
            ancho = (g["rumbo_hasta"] - g["rumbo_desde"]) % 360
            if (rumbo - g["rumbo_desde"]) % 360 < ancho:
                return z["zone_id"]
    return None


def agregar_tiempo(df, col_fecha):
    df["dia_semana"] = df[col_fecha].dt.dayofweek   # 0 = lunes
    df["hora"] = df[col_fecha].dt.hour
    df["mes"] = df[col_fecha].dt.month
    return df


# ================================================================== energía
def perfilar_energia(resumen):
    print("Energía (UCI)...")
    ruta = RAW / "uci" / "household_power_consumption.txt"
    df = pd.read_csv(ruta, sep=";", na_values="?", low_memory=False,
                     usecols=["Date", "Time", "Global_active_power", "Voltage"])
    filas = len(df)
    df = df.dropna()
    df["fecha"] = pd.to_datetime(df["Date"] + " " + df["Time"], format="%d/%m/%Y %H:%M:%S")

    # De minutos a horas: el promedio de kW en una hora = kWh consumidos en esa hora
    horario = (df.set_index("fecha")[["Global_active_power", "Voltage"]]
                 .resample("1h").mean().dropna().reset_index())
    horario = horario.rename(columns={"Global_active_power": "kw", "Voltage": "voltaje"})
    horario = agregar_tiempo(horario, "fecha")

    perfil_semanal(horario, ["kw", "voltaje"]).to_csv(SALIDA / "perfil_energia.csv", index=False)
    factor_mensual(horario, "kw").to_csv(SALIDA / "estacional_energia.csv", index=False)

    resumen["energia"] = {
        "filas_leidas": filas,
        "filas_validas": len(df),
        "pct_faltantes": round(100 * (1 - len(df) / filas), 2),
        "horas": len(horario),
        "desde": str(horario["fecha"].min()),
        "hasta": str(horario["fecha"].max()),
        "kw_promedio_hogar": round(horario["kw"].mean(), 3),
    }


# ================================================================ movilidad
def perfilar_trafico(resumen):
    print("Movilidad (CityPulse)...")
    centro = CONFIG["ciudad"]["centro"]

    # 1. Ubicar cada sensor real y asignarlo a una zona
    meta = pd.read_csv(RAW / "citypulse" / "trafficMetaData.csv")
    meta["lat"] = (meta["POINT_1_LAT"] + meta["POINT_2_LAT"]) / 2
    meta["lon"] = (meta["POINT_1_LNG"] + meta["POINT_2_LNG"]) / 2
    meta["distancia_km"], meta["rumbo"] = distancia_y_rumbo(
        centro["lat"], centro["lon"], meta["lat"], meta["lon"])
    meta["zone_id"] = [asignar_zona(d, r) for d, r in zip(meta["distancia_km"], meta["rumbo"])]
    mapa = meta[["REPORT_ID", "zone_id", "lat", "lon", "distancia_km", "rumbo",
                 "ROAD_TYPE", "DISTANCE_IN_METERS", "POINT_1_STREET"]].round(5)
    mapa.to_csv(SALIDA / "sensores_trafico_zona.csv", index=False)
    zona_de = dict(zip(meta["REPORT_ID"], meta["zone_id"]))

    # 2. Leer los 449 archivos (5 min) y llevarlos a horas por sensor
    columnas = ["status", "avgMeasuredTime", "avgSpeed", "TIMESTAMP", "vehicleCount", "REPORT_ID"]
    partes, filas, descartadas = [], 0, 0
    archivos = sorted((RAW / "citypulse" / "traffic").glob("trafficData*.csv"))
    for i, archivo in enumerate(archivos, 1):
        df = pd.read_csv(archivo, usecols=columnas)
        filas += len(df)
        ok = df[df["status"] == "OK"]
        descartadas += len(df) - len(ok)
        ok = ok.assign(fecha=pd.to_datetime(ok["TIMESTAMP"]).dt.floor("h"))
        partes.append(ok.groupby(["REPORT_ID", "fecha"]).agg(
            vehiculos=("vehicleCount", "sum"),          # 12 lecturas de 5 min -> veh/h
            velocidad=("avgSpeed", "mean"),
            tiempo=("avgMeasuredTime", "mean"),
            lecturas=("vehicleCount", "size"),
        ).reset_index())
        if i % 50 == 0:
            print(f"   {i}/{len(archivos)} archivos")
    horario = pd.concat(partes, ignore_index=True)
    # Solo horas completas (al menos 10 de las 12 lecturas de 5 min)
    horario = horario[horario["lecturas"] >= 10]
    horario["zone_id"] = horario["REPORT_ID"].map(zona_de)
    horario = agregar_tiempo(horario.dropna(subset=["zone_id"]), "fecha")

    perfil = perfil_semanal(horario, ["vehiculos", "velocidad", "tiempo"], grupo="zone_id")
    sensores_por_zona = horario.groupby("zone_id")["REPORT_ID"].nunique()
    perfil["sensores_reales"] = perfil["zone_id"].map(sensores_por_zona)

    # Zonas sin sensores reales: se usa el perfil promedio de toda la ciudad
    ciudad = perfil_semanal(horario, ["vehiculos", "velocidad", "tiempo"])
    for z in CONFIG["zonas"]:
        if z["zone_id"] not in sensores_por_zona.index:
            print(f"   AVISO: {z['zone_id']} no tiene sensores reales; usa el perfil de la ciudad")
            perfil = pd.concat([perfil, ciudad.assign(zone_id=z["zone_id"], sensores_reales=0)])
    perfil.sort_values(["zone_id", "dia_semana", "hora"]).to_csv(
        SALIDA / "perfil_trafico.csv", index=False)

    resumen["trafico"] = {
        "archivos": len(archivos),
        "filas_leidas": filas,
        "filas_descartadas_status": descartadas,
        "horas_sensor": len(horario),
        "desde": str(horario["fecha"].min()),
        "hasta": str(horario["fecha"].max()),
        "sensores_por_zona": {k: int(v) for k, v in sensores_por_zona.items()},
        "sin_zona": int(meta["zone_id"].isna().sum()),
    }


# ================================================================= ambiente
def perfilar_aire(resumen):
    print("Ambiente (OpenAQ)...")
    archivos = sorted((RAW / "openaq").glob("openaq_*.csv"))
    df = pd.concat([pd.read_csv(a) for a in archivos], ignore_index=True)
    filas = len(df)
    # Hora local de Dinamarca tal como viene en el texto (sin convertir a UTC)
    df["fecha"] = pd.to_datetime(df["datetime"].str[:19])
    df = df[df["value"] >= 0]  # valores negativos = error del equipo
    df = agregar_tiempo(df, "fecha")

    perfil = perfil_semanal(df.rename(columns={"value": "valor"}), ["valor"], grupo="parameter")
    perfil.rename(columns={"parameter": "parametro"}).to_csv(SALIDA / "perfil_aire.csv", index=False)
    factor_mensual(df, "value", grupo="parameter").rename(
        columns={"parameter": "parametro"}).to_csv(SALIDA / "estacional_aire.csv", index=False)

    stats = df.groupby("parameter")["value"].describe(percentiles=[0.5, 0.95, 0.99]).round(2)
    resumen["aire"] = {
        "estaciones": sorted(df["location_id"].unique().tolist()),
        "filas_leidas": filas,
        "por_parametro": stats.to_dict(orient="index"),
        "unidades": df.groupby("parameter")["units"].first().to_dict(),
    }


# ===================================================================== main
def main():
    SALIDA.mkdir(parents=True, exist_ok=True)
    resumen = {}
    perfilar_energia(resumen)
    perfilar_trafico(resumen)
    perfilar_aire(resumen)
    (SALIDA / "resumen_perfiles.json").write_text(
        json.dumps(resumen, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    print("\nSensores reales de tráfico por zona:", resumen["trafico"]["sensores_por_zona"])
    print(f"Archivos generados en {SALIDA}:")
    for f in sorted(SALIDA.iterdir()):
        print(f"   {f.name:<28} {f.stat().st_size / 1024:>7.1f} KB")


if __name__ == "__main__":
    main()
