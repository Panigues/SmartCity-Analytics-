"""
Generador de lecturas IoT de SmartCity.

Combina:
  - config/smartcity_config.json   (zonas, sensores, factores, correlaciones, ruido)
  - data/profiles/*.csv            (comportamiento típico extraído de UCI, CityPulse y OpenAQ)
  - los mismos 168 sensores que cargar_maestros.py instala en PostgreSQL

y produce una lectura por sensor por hora entre fecha_inicio y fecha_fin.

Salidas (data/generated/):
  hdfs/lecturas_AAAA_MM.csv        formato largo, una fila por variable  -> HDFS / Spark
  mongodb/lecturas_AAAA_MM.jsonl   un documento por lectura (anidado)    -> MongoDB
  postgres/lectura_diaria.csv      agregados diarios por sensor/variable -> tabla lectura_diaria
  anomalias_inyectadas.csv         "respuestas correctas" para validar la detección en Spark
  resumen_generacion.json          conteos para el documento técnico

Uso (desde la raíz del repo):
  python generator/generar_lecturas.py                    # simulación completa
  python generator/generar_lecturas.py --sensores 10 --meses 1   # prueba rápida
"""
import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
from cargar_maestros import construir_datos  # noqa: E402  (mismos sensores que PostgreSQL)

PERFILES = RAIZ / "data" / "profiles"
SALIDA = RAIZ / "data" / "generated"

# Variable principal de cada tipo: la que recibe las anomalías inyectadas
VARIABLE_PRINCIPAL = {
    "energia": "consumo_kwh", "calidad_aire": "pm10", "clima": "temperatura",
    "ruido": "nivel_db", "trafico": "vehiculos", "estacionamiento": "ocupacion_pct",
}
DOMINIO = {"CAT-ENE": "energia", "CAT-AMB": "ambiente", "CAT-MOV": "movilidad",
           "CAT-EST": "espacios_publicos", "CAT-TRP": "transporte"}


# ========================================================= carga de insumos
def cargar_perfiles():
    """Convierte cada perfil en una matriz [día_semana, hora] para indexar rápido."""
    def matriz(df, col):
        m = np.zeros((7, 24))
        m[df["dia_semana"].values, df["hora"].values] = df[col].values
        return m

    p = {}
    ene = pd.read_csv(PERFILES / "perfil_energia.csv")
    p["kw"] = matriz(ene, "kw_media")
    p["voltaje"] = matriz(ene, "voltaje_media")
    p["ene_mes"] = pd.read_csv(PERFILES / "estacional_energia.csv").set_index("mes")["factor"]

    tra = pd.read_csv(PERFILES / "perfil_trafico.csv")
    p["trafico"] = {z: {c: matriz(g, f"{c}_media") for c in ("vehiculos", "velocidad", "tiempo")}
                    for z, g in tra.groupby("zone_id")}

    aire = pd.read_csv(PERFILES / "perfil_aire.csv")
    p["aire"] = {par: matriz(g, "valor_media") for par, g in aire.groupby("parametro")}
    est = pd.read_csv(PERFILES / "estacional_aire.csv")
    p["aire_mes"] = {par: g.set_index("mes")["factor"] for par, g in est.groupby("parametro")}
    return p


# ===================================================== series compartidas
def clima_ciudad(t, rng):
    """Clima sintético de Aarhus: estacional + ciclo diario + frentes de varios días."""
    dia_anio = t.day_of_year.values
    hora = t.hour.values
    n = len(t)
    # Frentes meteorológicos: ruido suave (media móvil de 72 h)
    frente = np.convolve(rng.normal(0, 1, n + 72), np.ones(72) / 72 * 4, "valid")[:n]
    temp = (8.5 - 8.0 * np.cos(2 * np.pi * (dia_anio - 20) / 365)      # mín ~ ene, máx ~ jul
            + 3.5 * np.sin(2 * np.pi * (hora - 9) / 24)                 # máx ~ 15 h
            + frente + rng.normal(0, 0.6, n))
    hum = np.clip(80 - 1.2 * (temp - 8.5) - 8 * np.sin(2 * np.pi * (hora - 9) / 24)
                  + rng.normal(0, 4, n), 35, 100)
    presion = 1013 + np.cumsum(rng.normal(0, 0.4, n)) * 0.3
    presion = presion - pd.Series(presion).rolling(240, min_periods=1).mean().values + 1013
    viento = np.clip(rng.gamma(3, 1.5, n) + 0.5 * np.abs(frente), 0, 30)
    clim = pd.Series(temp).groupby([t.month.values, hora]).transform("mean").values
    return {"temperatura": temp, "humedad": hum, "presion": presion,
            "viento": viento, "temp_anomalia": temp - clim}


def factores_diarios(n_dias, rng, desv=0.12):
    """Variación día a día (un día con más o menos actividad que el típico)."""
    return np.clip(rng.normal(1, desv, n_dias), 0.6, 1.5)


# ===================================================== variables por tipo
def gen_energia(ctx, s, rng):
    z, t, p = ctx["zona"][s["zona"]], ctx["t"], ctx["perfiles"]
    dow, h, mes = ctx["dow"], ctx["hora"], ctx["mes"]
    cfg = ctx["cfg"]["perfiles_energia"]

    residencial = p["kw"][dow, h] / p["kw"].mean()
    curvas = {"residencial": residencial}
    for tipo in ("comercial", "industrial"):
        hora_f = np.array(cfg[tipo]["horario"])[h]
        ds = cfg[tipo]["dia_semana"]
        dia_f = np.where(dow < 5, ds["lun-vie"], np.where(dow == 5, ds["sab"], ds["dom"]))
        curva = hora_f * dia_f
        curvas[tipo] = curva / curva.mean()
    mezcla = sum(peso * curvas[k] for k, peso in cfg["mezclas"][z["perfil"]].items())

    hogares = z["hogares"] / z["sensores"]["energia"]
    estacional = p["ene_mes"].reindex(mes).values
    efecto_temp = 1 - ctx["cfg"]["correlaciones"]["temperatura_a_consumo"] * 0.05 * ctx["clima"]["temp_anomalia"]
    consumo = (ctx["kw_hogar"] * hogares * z["factores"]["energia"] * mezcla * estacional
               * efecto_temp * ctx["dia_ene"][s["zona"]][ctx["dia"]] * s["mult"]
               * rng.lognormal(0, 0.08, len(t)))
    demanda = consumo * rng.uniform(1.05, 1.25, len(t))
    voltaje = p["voltaje"][dow, h] - 1.5 * (mezcla - 1) + rng.normal(0, 1.2, len(t))
    return {"consumo_kwh": consumo, "demanda_kw": demanda, "voltaje": voltaje}


def gen_calidad_aire(ctx, s, rng):
    z, p = ctx["zona"][s["zona"]], ctx["perfiles"]
    dow, h, mes, n = ctx["dow"], ctx["hora"], ctx["mes"], len(ctx["t"])
    f = z["factores"]["contaminacion"] * s["mult"]
    dia_tra = ctx["dia_tra"][s["zona"]][ctx["dia"]]
    corr = ctx["cfg"]["correlaciones"]["trafico_a_no2"]
    # Viento fuerte dispersa los contaminantes
    dispersion = np.clip(1.25 - 0.04 * ctx["clima"]["viento"], 0.6, 1.25)

    def base(par):
        return p["aire"][par][dow, h] * p["aire_mes"][par].reindex(mes).values * f * dispersion

    pm10 = base("pm10") * rng.lognormal(0, 0.30, n)
    pm10[ctx["anio_nuevo"]] *= rng.uniform(6, 12)          # fuegos artificiales 1 de enero
    no2 = base("no2") * (1 + corr * (dia_tra - 1)) * rng.lognormal(0, 0.22, n)
    co = base("co") / 1000 * rng.lognormal(0, 0.15, n)       # µg/m³ -> mg/m³
    rel = ctx["tra_rel"][s["zona"]]
    co2 = 415 + 45 * z["factores"]["contaminacion"] * rel * dia_tra + rng.normal(0, 10, n)
    return {"pm25": pm10 * rng.uniform(0.55, 0.75, n), "pm10": pm10,
            "no2": no2, "co": co, "co2": co2}


def gen_clima(ctx, s, rng):
    c, n = ctx["clima"], len(ctx["t"])
    isla_calor = 0.8 if s["zona"] == "Z01" else 0.0          # el centro es más cálido
    return {"temperatura": c["temperatura"] + isla_calor + rng.normal(0, 0.3, n),
            "humedad": np.clip(c["humedad"] + rng.normal(0, 1.5, n), 20, 100),
            "presion": c["presion"] + rng.normal(0, 0.4, n),
            "viento": np.clip(c["viento"] * s["mult"] + rng.normal(0, 0.5, n), 0, None)}


def gen_ruido(ctx, s, rng):
    z = ctx["zona"][s["zona"]]
    veh = ctx["tra_esperado"][s["zona"]] * ctx["dia_tra"][s["zona"]][ctx["dia"]]
    corr = ctx["cfg"]["correlaciones"]["trafico_a_ruido"]
    nivel = (42 + 13 * corr / 0.7 * np.log10(1 + veh)
             + 6 * (z["factores"]["ruido"] - 1) + 10 * np.log10(s["mult"])
             + rng.normal(0, 2.5, len(veh)))
    return {"nivel_db": nivel}


def gen_trafico(ctx, s, rng):
    zid = s["zona"]
    perf = ctx["perfiles"]["trafico"][zid]
    dow, h, n = ctx["dow"], ctx["hora"], len(ctx["t"])
    dia_tra = ctx["dia_tra"][zid][ctx["dia"]]
    esperado = ctx["tra_esperado"][zid] * dia_tra * s["mult"]
    vehiculos = rng.poisson(np.maximum(esperado, 0)).astype(float)
    # Más tráfico que lo normal -> menor velocidad
    velocidad = np.clip(perf["velocidad"][dow, h] * (1 - 0.6 * (dia_tra - 1))
                        + rng.normal(0, 4, n), 3, 120)
    tiempo = perf["tiempo"][dow, h] * perf["velocidad"][dow, h] / velocidad * s["mult"]
    return {"vehiculos": vehiculos, "velocidad_promedio": velocidad, "tiempo_recorrido": tiempo}


def gen_estacionamiento(ctx, s, rng):
    perfil = ctx["zona"][s["zona"]]["perfil"]
    h, dow, n = ctx["hora"], ctx["dow"], len(ctx["t"])
    campana = np.exp(-((h - 13) / 4.0) ** 2)               # pico al mediodía
    finde = dow >= 5
    if perfil in ("comercial", "mixto_alta_densidad"):
        occ = 15 + 80 * campana * np.where(finde, 1.05, 0.95)
    elif perfil == "industrial":
        occ = 10 + 75 * campana * np.where(finde, 0.3, 1.0)
    else:                                                    # residencial: lleno de noche
        occ = 90 - 55 * campana * np.where(finde, 0.5, 1.0)
    occ = occ * ctx["dia_tra"][s["zona"]][ctx["dia"]] * s["mult"] + rng.normal(0, 4, n)
    return {"ocupacion_pct": np.clip(occ, 0, 100)}


GENERADORES = {"energia": gen_energia, "calidad_aire": gen_calidad_aire, "clima": gen_clima,
               "ruido": gen_ruido, "trafico": gen_trafico, "estacionamiento": gen_estacionamiento}


# ================================================== defectos realistas
def aplicar_defectos(ctx, s, valores, rng):
    """Anomalías, datos faltantes y sensores que dejan de reportar."""
    n = len(ctx["t"])
    ruido = ctx["cfg"]["ruido_simulacion"]

    # Anomalías: picos en la variable principal
    principal = VARIABLE_PRINCIPAL[s["tipo"]]
    idx_anom = np.flatnonzero(rng.random(n) < ruido["probabilidad_anomalia"])
    anomalias = []
    for i in idx_anom:
        antes = valores[principal][i]
        if s["tipo"] == "clima":
            valores[principal][i] += rng.choice([-1, 1]) * rng.uniform(15, 25)
        elif s["tipo"] == "estacionamiento":
            valores[principal][i] = 100.0
        elif s["tipo"] == "ruido":
            valores[principal][i] += rng.uniform(20, 30)
        else:
            valores[principal][i] *= rng.uniform(4, 8)
        anomalias.append((s["nombre"], ctx["t"][i], principal, antes, valores[principal][i]))

    # Lecturas perdidas
    valido = rng.random(n) >= ruido["probabilidad_dato_faltante"]

    # Sensores inactivos dejan de reportar en una fecha de 2025;
    # en mantenimiento, dejan de reportar en las últimas semanas.
    fin = ctx["t"][-1]
    if s["estado"] == "inactivo":
        corte = pd.Timestamp("2025-02-01") + pd.Timedelta(days=int(rng.integers(0, 300)))
        valido &= ctx["t"] < corte
    elif s["estado"] == "mantenimiento":
        corte = fin - pd.Timedelta(days=int(rng.integers(5, 21)))
        valido &= ctx["t"] < corte
    return valido, anomalias


# ======================================================== escritura
def escribir(ctx, s, valores, valido, archivos):
    t = ctx["t"][valido]
    meses = ctx["clave_mes"][valido]
    unidades = ctx["cfg"]["tipos_sensor"][s["tipo"]]["variables"]
    zona = ctx["zona"][s["zona"]]
    v = {k: np.round(a[valido], 0 if k == "vehiculos" else 3) for k, a in valores.items()}
    ts_txt = t.strftime("%Y-%m-%dT%H:%M:%SZ").values

    for clave in np.unique(meses):
        m = meses == clave

        if "hdfs" in ctx["formatos"]:
            largo = pd.concat([
                pd.DataFrame({"timestamp": ts_txt[m], "zone_id": s["zona"], "domain": s["dominio"],
                              "tipo": s["tipo"], "sensor_id": s["nombre"], "variable": var,
                              "value": v[var][m], "unit": unidades[var]})
                for var in unidades], ignore_index=True)
            ruta = archivos["hdfs"] / f"lecturas_{clave}.csv"
            largo.to_csv(ruta, mode="a", header=not ruta.exists(), index=False)

        if "mongodb" in ctx["formatos"]:
            lon, lat = s["lon"], s["lat"]
            cabecera = (f'{{"sensor_id":"{s["nombre"]}","id_sensor":{s["id"]},"tipo":"{s["tipo"]}",'
                        f'"categoria":"{s["dominio"]}","zona":{{"id":"{s["zona"]}","nombre":"{zona["nombre"]}"}},'
                        f'"ubicacion":{{"type":"Point","coordinates":[{lon},{lat}]}},')
            cols = list(unidades)
            filas = zip(ts_txt[m], *(v[c][m] for c in cols))
            lineas = [cabecera + f'"timestamp":{{"$date":"{f[0]}"}},"mediciones":{{'
                      + ",".join(f'"{c}":{x:g}' for c, x in zip(cols, f[1:])) + "}}\n"
                      for f in filas]
            with open(archivos["mongodb"] / f"lecturas_{clave}.jsonl", "a", encoding="utf-8") as fh:
                fh.writelines(lineas)

    if "postgres" in ctx["formatos"]:
        df = pd.DataFrame(v)
        df["fecha"] = t.date
        diario = df.groupby("fecha").agg(["mean", "min", "max", "count"])
        partes = []
        for var in unidades:
            d = diario[var].reset_index()
            d.columns = ["fecha", "promedio", "minimo", "maximo", "cantidad"]
            d.insert(0, "id_sensor", s["id"])
            d.insert(2, "variable", var)   # mismo orden de columnas que la tabla
            partes.append(d)
        diario = pd.concat(partes).round({"promedio": 4, "minimo": 4, "maximo": 4})
        ruta = archivos["postgres"] / "lectura_diaria.csv"
        diario.to_csv(ruta, mode="a", header=not ruta.exists(), index=False)


# ============================================================== main
def main():
    ap = argparse.ArgumentParser(description="Generador de lecturas IoT de SmartCity")
    ap.add_argument("--sensores", type=int, help="limitar a los primeros N sensores (prueba)")
    ap.add_argument("--meses", type=int, help="limitar a los primeros N meses (prueba)")
    ap.add_argument("--formatos", nargs="+", default=["hdfs", "mongodb", "postgres"],
                    choices=["hdfs", "mongodb", "postgres"])
    args = ap.parse_args()
    inicio_reloj = time.time()

    cfg = json.loads((RAIZ / "config" / "smartcity_config.json").read_text(encoding="utf-8"))
    sim = cfg["simulacion"]
    rng = np.random.default_rng(sim["semilla"])
    perfiles = cargar_perfiles()
    resumen_perf = json.loads((PERFILES / "resumen_perfiles.json").read_text(encoding="utf-8"))

    fin = pd.Timestamp(sim["fecha_fin"])
    if args.meses:
        fin = min(fin, pd.Timestamp(sim["fecha_inicio"]) + pd.DateOffset(months=args.meses)
                  - pd.Timedelta(hours=1))
    t = pd.date_range(sim["fecha_inicio"], fin, freq="h")
    dias = (t.normalize() - t[0].normalize()).days.values
    n_dias = dias.max() + 1

    # Sensores: exactamente los mismos que cargar_maestros.py inserta en PostgreSQL
    datos = construir_datos(cfg)
    coords = {u[0]: (u[4], u[3]) for u in datos["ubicaciones"]}   # código -> (lon, lat)
    cat_de_tipo = {k: v["id_categoria"] for k, v in cfg["tipos_sensor"].items()}
    sensores = []
    for i, (nombre, tipo, ubic, _org, _fecha, estado) in enumerate(datos["sensores"], start=1):
        lon, lat = coords[ubic]
        sensores.append({"id": i, "nombre": nombre, "tipo": tipo, "zona": nombre[:3],
                         "estado": estado, "lon": lon, "lat": lat,
                         "dominio": DOMINIO[cat_de_tipo[tipo]]})
    if args.sensores:
        sensores = sensores[:args.sensores]

    zonas = {z["zone_id"]: z for z in cfg["zonas"]}
    dow, hora = t.day_of_week.values, t.hour.values
    tra_esperado = {z: perfiles["trafico"][z]["vehiculos"][dow, hora] for z in zonas}
    ctx = {
        "cfg": cfg, "perfiles": perfiles, "t": t, "dow": dow, "hora": hora,
        "mes": t.month.values, "dia": dias, "zona": zonas, "formatos": args.formatos,
        "clave_mes": t.strftime("%Y_%m").values,
        "kw_hogar": resumen_perf["energia"]["kw_promedio_hogar"],
        "clima": clima_ciudad(t, rng),
        "dia_tra": {z: factores_diarios(n_dias, rng) for z in zonas},
        "dia_ene": {z: factores_diarios(n_dias, rng, 0.06) for z in zonas},
        "tra_esperado": tra_esperado,
        "tra_rel": {z: tra_esperado[z] / tra_esperado[z].mean() for z in zonas},
        "anio_nuevo": (t.month == 1) & (t.day == 1) & (t.hour <= 2),
    }

    # Preparar carpetas (se borran las salidas anteriores)
    archivos = {f: SALIDA / f for f in args.formatos}
    for carpeta in archivos.values():
        carpeta.mkdir(parents=True, exist_ok=True)
        for viejo in carpeta.glob("lecturas_*"):
            viejo.unlink()
        (carpeta / "lectura_diaria.csv").unlink(missing_ok=True)

    variacion = cfg["ruido_simulacion"]["variacion_entre_sensores_pct"] / 100
    todas_anomalias, conteo = [], {"lecturas": 0, "filas_largas": 0, "faltantes": 0}
    print(f"Generando {len(sensores)} sensores x {len(t):,} horas "
          f"({t[0]:%Y-%m-%d} a {t[-1]:%Y-%m-%d})")
    for k, s in enumerate(sensores, 1):
        srng = np.random.default_rng([sim["semilla"], s["id"]])   # reproducible por sensor
        s["mult"] = float(np.clip(srng.normal(1, variacion), 0.7, 1.3))
        valores = GENERADORES[s["tipo"]](ctx, s, srng)
        valido, anomalias = aplicar_defectos(ctx, s, valores, srng)
        escribir(ctx, s, valores, valido, archivos)
        todas_anomalias += anomalias
        conteo["lecturas"] += int(valido.sum())
        conteo["filas_largas"] += int(valido.sum()) * len(valores)
        conteo["faltantes"] += int((~valido).sum())
        if k % 20 == 0 or k == len(sensores):
            print(f"   {k}/{len(sensores)} sensores  ({time.time() - inicio_reloj:.0f} s)")

    SALIDA.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(todas_anomalias, columns=["sensor", "timestamp", "variable",
                                           "valor_normal", "valor_anomalo"]).round(
        {"valor_normal": 3, "valor_anomalo": 3}).to_csv(
        SALIDA / "anomalias_inyectadas.csv", index=False)

    tamanos = {f: round(sum(a.stat().st_size for a in c.iterdir()) / 1024 ** 2, 1)
               for f, c in archivos.items()}
    resumen = {"sensores": len(sensores), "horas": len(t), "desde": str(t[0]), "hasta": str(t[-1]),
               **conteo, "anomalias": len(todas_anomalias), "tamano_mb": tamanos,
               "segundos": round(time.time() - inicio_reloj, 1)}
    (SALIDA / "resumen_generacion.json").write_text(json.dumps(resumen, indent=2), encoding="utf-8")
    print("\nResumen:")
    for clave, valor in resumen.items():
        print(f"   {clave:<14} {valor}")


if __name__ == "__main__":
    main()
