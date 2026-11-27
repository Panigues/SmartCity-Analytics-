"""
SmartCity Analytics - Dashboard (Streamlit)

Integra las cuatro tecnologías del proyecto:
  PostgreSQL  -> datos maestros, agregados diarios y consultas SQL
  MongoDB     -> lecturas IoT (iot_readings) y consultas con Aggregation Framework
  Hadoop      -> estado de HDFS y resultados de los jobs MapReduce
  Spark       -> indicadores y anomalías calculados por el ETL (escritos en PostgreSQL)

Ejecutar (desde la raíz del repo, con los contenedores arriba):
    pip install -r dashboard/requirements.txt
    streamlit run dashboard/app.py
"""
import os
import subprocess
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

RAIZ = Path(__file__).resolve().parent.parent

# ============================================================= configuración
def leer_env():
    valores = {}
    ruta = RAIZ / ".env"
    if ruta.exists():
        for linea in ruta.read_text(encoding="utf-8").splitlines():
            linea = linea.strip()
            if linea and not linea.startswith("#") and "=" in linea:
                k, v = linea.split("=", 1)
                valores[k.strip()] = v.strip()
    valores.update({k: v for k, v in os.environ.items() if k.startswith(("POSTGRES_", "PG_", "MONGO", "HDFS"))})
    return valores


ENV = leer_env()
PG = {
    "host": ENV.get("PG_HOST", "localhost"),
    "port": ENV.get("PG_PORT", "5433"),
    "db": ENV.get("POSTGRES_DB", "smartcity"),
    "user": ENV.get("POSTGRES_USER", ENV.get("PG_USER", "smartcity")),
    "password": ENV.get("POSTGRES_PASSWORD", ENV.get("PG_PASSWORD", "")),
}
MONGO_URI = ENV.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = ENV.get("MONGO_DB", "smartcity")
HDFS_CONTENEDOR = ENV.get("HDFS_CONTAINER", "namenode")

LIMITES = {"pm25": 35, "pm10": 50, "no2": 200, "co": 10, "co2": 1000,
           "nivel_db": 70, "ocupacion_pct": 95}
INICIO_DATOS, FIN_DATOS = date(2024, 1, 1), date(2025, 12, 31)
COLORES_ZONA = {"Centro": "#4C6EF5", "Norte": "#12B886", "Sur": "#FA5252",
                "Este": "#FAB005", "Oeste": "#7950F2"}

st.set_page_config(page_title="SmartCity Analytics", layout="wide")
st.markdown("""
<style>
.block-container {padding-top: 3.2rem;}
div[data-testid="stMetric"] {background: rgba(128,128,128,0.07); border-radius: 10px; padding: 10px 14px;}
div[data-testid="stMetricLabel"] p {font-size: 0.82rem;}
div[data-testid="stMetricValue"] {font-size: 1.55rem;}
.fuente {display:inline-block; font-size:0.72rem; font-weight:600; padding:2px 8px;
         border-radius:10px; margin-bottom:4px; color:white;}
.f-PostgreSQL {background:#336791;} .f-MongoDB {background:#13AA52;}
.f-HDFS, .f-Hadoop {background:#E5A00D;} .f-Spark {background:#E25A1C;}
</style>""", unsafe_allow_html=True)


def etiqueta(*fuentes):
    """Etiqueta de origen de los datos (requisito del enunciado)."""
    html = " ".join(f'<span class="fuente f-{f.split()[0]}">{f}</span>' for f in fuentes)
    st.markdown(html, unsafe_allow_html=True)


# ============================================================== conexiones
@st.cache_resource
def motor_pg():
    from sqlalchemy import create_engine
    url = f"postgresql+psycopg2://{PG['user']}:{PG['password']}@{PG['host']}:{PG['port']}/{PG['db']}"
    return create_engine(url, pool_pre_ping=True)


def sql(consulta, params=None):
    from sqlalchemy import text
    with motor_pg().connect() as c:
        return pd.read_sql(text(consulta), c, params=params)


@st.cache_resource
def cliente_mongo():
    from pymongo import MongoClient
    cli = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2500)
    cli.admin.command("ping")
    return cli


def mongo():
    return cliente_mongo()[MONGO_DB]


def hdfs(comando):
    """Ejecuta un comando hdfs dentro del contenedor del NameNode."""
    r = subprocess.run(["docker", "exec", HDFS_CONTENEDOR, "bash", "-c", comando],
                       capture_output=True, text=True, timeout=60)
    lineas = [l for l in r.stdout.splitlines() if l.strip() and " INFO " not in l and " WARN " not in l]
    if r.returncode != 0 and not lineas:
        raise RuntimeError(r.stderr.strip()[-300:] or "error al ejecutar en HDFS")
    return lineas


def estado_conexiones():
    estado = {}
    try:
        sql("SELECT 1"); estado["PostgreSQL"] = True
    except Exception:
        estado["PostgreSQL"] = False
    try:
        cliente_mongo(); estado["MongoDB"] = True
    except Exception:
        estado["MongoDB"] = False
    try:
        hdfs("hdfs dfs -test -d /smartcity && echo ok"); estado["HDFS"] = True
    except Exception:
        estado["HDFS"] = False
    return estado


# ============================================================ datos (cache)
@st.cache_data(ttl=600, show_spinner="Cargando agregados diarios desde PostgreSQL…")
def datos_diarios():
    return sql("""
        SELECT l.fecha, l.variable, l.promedio::float AS promedio, l.minimo::float AS minimo,
               l.maximo::float AS maximo, l.cantidad, s.id_sensor, s.nombre AS sensor,
               t.nombre AS tipo, z.id_zona, z.nombre AS zona
        FROM lectura_diaria l
        JOIN sensor s      ON s.id_sensor = l.id_sensor
        JOIN tipo_sensor t ON t.id_tipo = s.id_tipo
        JOIN ubicacion u   ON u.id_ubicacion = s.id_ubicacion
        JOIN zona z        ON z.id_zona = u.id_zona""")


@st.cache_data(ttl=600)
def catalogo():
    sensores = sql("""SELECT s.id_sensor, s.nombre AS sensor, t.nombre AS tipo, c.nombre_categoria AS categoria,
                             z.id_zona, z.nombre AS zona, u.latitud::float AS lat, u.longitud::float AS lon,
                             s.estado, o.nombre AS organismo
                      FROM sensor s JOIN tipo_sensor t ON t.id_tipo = s.id_tipo
                      JOIN categoria c ON c.id_categoria = t.id_categoria
                      JOIN ubicacion u ON u.id_ubicacion = s.id_ubicacion
                      JOIN zona z ON z.id_zona = u.id_zona
                      JOIN organismo o ON o.id_organismo = s.id_organismo""")
    return sensores


@st.cache_data(ttl=600)
def indicadores_spark():
    return sql("""SELECT i.id_zona, z.nombre AS zona, i.anio, i.mes, i.variable,
                         i.promedio::float AS promedio, i.maximo::float AS maximo, i.lecturas, i.anomalias
                  FROM indicador_zona_mensual i JOIN zona z ON z.id_zona = i.id_zona""")


@st.cache_data(ttl=600)
def anomalias_spark():
    return sql("""SELECT fecha_hora, sensor, tipo, zona, id_zona, variable,
                         valor::float AS valor, esperado::float AS esperado, puntaje::float AS puntaje
                  FROM v_anomalias""")


def rango_mongo(desde, hasta):
    return {"$gte": datetime.combine(desde, time.min), "$lt": datetime.combine(hasta + timedelta(days=1), time.min)}


@st.cache_data(ttl=600, show_spinner="Consultando MongoDB…")
def mongo_kpis(desde, hasta, zonas):
    """KPIs que salen de las lecturas horarias de MongoDB."""
    col = mongo()["iot_readings"]
    base = {"timestamp": rango_mongo(desde, hasta), "zona.id": {"$in": list(zonas)}}
    total = col.count_documents(base)
    congestion = col.count_documents({**base, "tipo": "trafico", "mediciones.velocidad_promedio": {"$lt": 15}})
    trafico = col.count_documents({**base, "tipo": "trafico"})
    alertas_amb = col.count_documents({**base, "$or": [
        {"tipo": "calidad_aire", f"mediciones.{v}": {"$gt": LIMITES[v]}} for v in ("pm25", "pm10", "no2", "co", "co2")
    ] + [{"tipo": "ruido", "mediciones.nivel_db": {"$gt": 70}}]})
    por_hora = list(col.aggregate([
        {"$match": {**base, "tipo": "energia"}},
        {"$group": {"_id": {"$hour": "$timestamp"}, "consumo": {"$avg": "$mediciones.consumo_kwh"}}},
        {"$sort": {"_id": 1}}]))
    return total, congestion, trafico, alertas_amb, pd.DataFrame(por_hora).rename(columns={"_id": "hora"})


# =================================================================== filtros
st.sidebar.title("SmartCity Analytics")
st.sidebar.caption("Aarhus · 5 zonas · 168 sensores IoT")
estado = estado_conexiones()
st.sidebar.markdown("**Conexiones**")
st.sidebar.markdown("  \n".join(f"{':green[●]' if ok else ':red[●]'} {k}" for k, ok in estado.items()))

if not estado["PostgreSQL"]:
    st.error(f"No se pudo conectar a PostgreSQL en {PG['host']}:{PG['port']}. "
             "Revisa que el contenedor esté arriba y que el archivo .env tenga la contraseña correcta.")
    st.stop()

diario = datos_diarios()
cat = catalogo()
zonas_todas = ["Centro", "Norte", "Sur", "Este", "Oeste"]
tipos_todos = sorted(cat["tipo"].unique())

st.sidebar.markdown("---")
st.sidebar.markdown("**Filtros**")
periodo = st.sidebar.date_input("Período", (date(2025, 1, 1), FIN_DATOS),
                                min_value=INICIO_DATOS, max_value=FIN_DATOS)
desde, hasta = (periodo if isinstance(periodo, (tuple, list)) and len(periodo) == 2 else (INICIO_DATOS, FIN_DATOS))
zonas = st.sidebar.multiselect("Zona", zonas_todas, default=zonas_todas)
tipos = st.sidebar.multiselect("Tipo de sensor", tipos_todos, default=tipos_todos)
variables = sorted(diario.loc[diario["tipo"].isin(tipos), "variable"].unique()) or sorted(diario["variable"].unique())
variable = st.sidebar.selectbox("Variable de la serie temporal", variables,
                                index=variables.index("pm25") if "pm25" in variables else 0)
sensores_op = ["Todos"] + sorted(cat.loc[cat["zona"].isin(zonas) & cat["tipo"].isin(tipos), "sensor"])
sensor_sel = st.sidebar.selectbox("Sensor", sensores_op)
if not zonas or not tipos:
    st.warning("Selecciona al menos una zona y un tipo de sensor.")
    st.stop()
ids_zona = cat.loc[cat["zona"].isin(zonas), "id_zona"].unique().tolist()

f = diario[(diario["fecha"] >= desde) & (diario["fecha"] <= hasta) & diario["zona"].isin(zonas)]
if sensor_sel != "Todos":
    f = f[f["sensor"] == sensor_sel]


def prom(var):
    d = f[f["variable"] == var]
    return (d["promedio"] * d["cantidad"]).sum() / d["cantidad"].sum() if d["cantidad"].sum() else None


def suma(var):
    d = f[f["variable"] == var]
    return (d["promedio"] * d["cantidad"]).sum()


def fmt(x, dec=1, suf=""):
    return "—" if x is None or pd.isna(x) else f"{x:,.{dec}f}{suf}".replace(",", " ")


# ================================================================ páginas
pag_dash, pag_consultas = st.tabs(["Dashboard", "Consultas por tecnología"])

with pag_dash:
    st.title("Panel de la ciudad")
    st.caption(f"Período: {desde:%d/%m/%Y} – {hasta:%d/%m/%Y} · Zonas: {', '.join(zonas)}"
               + (f" · Sensor: {sensor_sel}" if sensor_sel != "Todos" else ""))

    try:
        tot_mongo, congest, traf_total, alertas_amb, consumo_hora = mongo_kpis(desde, hasta, tuple(ids_zona)) \
            if estado["MongoDB"] else (None, None, None, None, pd.DataFrame())
    except Exception as e:
        st.warning(f"MongoDB no respondió: {e}")
        tot_mongo, congest, traf_total, alertas_amb, consumo_hora = None, None, None, None, pd.DataFrame()

    anom = anomalias_spark()
    anom_f = anom[(anom["fecha_hora"].dt.date >= desde) & (anom["fecha_hora"].dt.date <= hasta) & anom["zona"].isin(zonas)]

    # ---------------- KPIs
    st.subheader("Movilidad")
    etiqueta("PostgreSQL", "MongoDB", "Spark")
    c = st.columns(5)
    c[0].metric("Vehículos detectados", fmt(suma("vehiculos"), 0))
    c[1].metric("Velocidad promedio", fmt(prom("velocidad_promedio"), 1, " km/h"))
    c[2].metric("Congestión promedio", fmt(100 * congest / traf_total if traf_total else None, 1, " %"),
                help="Porcentaje de lecturas de tráfico con velocidad < 15 km/h (MongoDB)")
    c[3].metric("Incidentes de tráfico", fmt(len(anom_f[anom_f["tipo"] == "trafico"]), 0),
                help="Anomalías de tráfico detectadas por Spark")
    c[4].metric("Tiempo promedio de recorrido", fmt(prom("tiempo_recorrido"), 0, " s"))

    st.subheader("Ambiente")
    etiqueta("PostgreSQL", "MongoDB")
    c = st.columns(5)
    c[0].metric("Temperatura promedio", fmt(prom("temperatura"), 1, " °C"))
    c[1].metric("PM2.5 promedio", fmt(prom("pm25"), 1, " µg/m³"))
    c[2].metric("CO₂ promedio", fmt(prom("co2"), 0, " ppm"))
    c[3].metric("Humedad promedio", fmt(prom("humedad"), 0, " %"))
    c[4].metric("Alertas ambientales", fmt(alertas_amb, 0),
                help="Lecturas sobre el límite: PM2.5>35, PM10>50, NO₂>200, CO>10, CO₂>1000, ruido>70 dB (MongoDB)")

    st.subheader("Energía")
    etiqueta("PostgreSQL", "MongoDB")
    total_kwh = suma("consumo_kwh")
    dias = (hasta - desde).days + 1
    ant = diario[(diario["fecha"] >= desde - timedelta(days=dias)) & (diario["fecha"] < desde)
                 & diario["zona"].isin(zonas) & (diario["variable"] == "consumo_kwh")]
    total_ant = (ant["promedio"] * ant["cantidad"]).sum()
    var_pct = 100 * (total_kwh - total_ant) / total_ant if total_ant else None
    ene = f[f["variable"] == "consumo_kwh"]
    por_zona = (ene.assign(t=ene["promedio"] * ene["cantidad"]).groupby("zona")["t"].sum() / max(dias, 1))
    hora_pico = consumo_hora.loc[consumo_hora["consumo"].idxmax(), "hora"] if not consumo_hora.empty else None
    c = st.columns(5)
    c[0].metric("Consumo total", fmt(total_kwh / 1000, 0, " MWh"))
    c[1].metric("Consumo promedio por zona", fmt(por_zona.mean() / 1000 if len(por_zona) else None, 1, " MWh/día"))
    c[2].metric("Hora de mayor consumo", "—" if hora_pico is None else f"{int(hora_pico):02d}:00",
                help="Promedio de consumo por hora del día (MongoDB)")
    c[3].metric("Pico de consumo", fmt(ene["maximo"].max() if len(ene) else None, 0, " kWh"),
                help="Mayor lectura horaria de un medidor en el período")
    c[4].metric("Variación vs período anterior", fmt(var_pct, 1, " %"),
                help="Consumo total del período contra el período anterior de la misma duración")

    st.subheader("IoT")
    etiqueta("PostgreSQL", "MongoDB", "Spark")
    cat_f = cat[cat["zona"].isin(zonas) & cat["tipo"].isin(tipos)]
    mediciones = tot_mongo if tot_mongo is not None else int(f.groupby(["sensor", "fecha"])["cantidad"].max().sum())
    segundos = dias * 86400
    c = st.columns(6)
    c[0].metric("Sensores", fmt(len(cat_f), 0))
    c[1].metric("Activos / inactivos", f"{(cat_f['estado'] == 'activo').sum()} / {(cat_f['estado'] != 'activo').sum()}",
                help="Inactivos incluye sensores en mantenimiento")
    c[2].metric("Mediciones recibidas", fmt(mediciones, 0))
    c[3].metric("Alertas generadas", fmt(len(anom_f) + (alertas_amb or 0), 0),
                help="Anomalías detectadas por Spark + alertas por umbral")
    c[4].metric("Datos por hora / día", f"{fmt(mediciones / (dias * 24), 0)} / {fmt(mediciones / dias, 0)}")
    c[5].metric("Lecturas por segundo", fmt(mediciones / segundos, 3))

    st.markdown("---")

    # ---------------- gráficos
    g1, g2 = st.columns(2)
    with g1:
        st.markdown(f"**1. Serie temporal – {variable}** (promedio diario por zona)")
        etiqueta("PostgreSQL")
        serie = f[f["variable"] == variable]
        serie = (serie.assign(t=serie["promedio"] * serie["cantidad"]).groupby(["fecha", "zona"])
                 .agg(t=("t", "sum"), n=("cantidad", "sum")).reset_index())
        serie["valor"] = serie["t"] / serie["n"]
        if serie.empty:
            st.info("Sin datos para esta variable con los filtros actuales.")
        else:
            fig = px.line(serie, x="fecha", y="valor", color="zona", color_discrete_map=COLORES_ZONA,
                          labels={"valor": variable, "fecha": ""})
            fig.update_layout(height=340, margin=dict(l=0, r=0, t=10, b=0), legend_title_text="")
            st.plotly_chart(fig, width="stretch")
    with g2:
        st.markdown("**2. Comparativa por zonas** (promedio en el período)")
        etiqueta("PostgreSQL")
        comp = f[f["variable"] == variable]
        comp = (comp.assign(t=comp["promedio"] * comp["cantidad"]).groupby("zona")
                .agg(t=("t", "sum"), n=("cantidad", "sum")).reset_index())
        comp["valor"] = comp["t"] / comp["n"]
        fig = px.bar(comp.sort_values("valor", ascending=False), x="zona", y="valor", color="zona",
                     color_discrete_map=COLORES_ZONA, labels={"valor": variable, "zona": ""}, text_auto=".1f")
        if variable in LIMITES:
            fig.add_hline(y=LIMITES[variable], line_dash="dash", line_color="red",
                          annotation_text=f"límite {LIMITES[variable]}")
        fig.update_layout(height=340, margin=dict(l=0, r=0, t=10, b=0), showlegend=False)
        st.plotly_chart(fig, width="stretch")

    g3, g4 = st.columns(2)
    with g3:
        st.markdown("**3. Distribución de sensores por tipo**")
        etiqueta("PostgreSQL")
        dist = cat_f.groupby("tipo").size().reset_index(name="sensores")
        fig = px.pie(dist, names="tipo", values="sensores", hole=0.5)
        fig.update_layout(height=340, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, width="stretch")
    with g4:
        st.markdown("**4. Resultado analítico de Spark** – zonas con mayor incremento de contaminación")
        etiqueta("Spark", "HDFS")
        ind = indicadores_spark()
        contam = st.selectbox("Contaminante", ["pm25", "pm10", "no2", "co2"], key="contam",
                              label_visibility="collapsed")
        i = ind[(ind["variable"] == contam) & ind["zona"].isin(zonas)].copy()
        i["periodo"] = i["anio"] * 100 + i["mes"]
        p_ini, p_fin = desde.year * 100 + desde.month, hasta.year * 100 + hasta.month
        i = i[(i["periodo"] >= p_ini) & (i["periodo"] <= p_fin)]
        if i["periodo"].nunique() < 2:
            st.info("Selecciona un período de al menos dos meses.")
        else:
            primero = i[i["periodo"] == i["periodo"].min()].set_index("zona")["promedio"]
            ultimo = i[i["periodo"] == i["periodo"].max()].set_index("zona")["promedio"]
            cambio = (100 * (ultimo - primero) / primero).dropna().sort_values(ascending=False).reset_index()
            cambio.columns = ["zona", "variacion"]
            cambio["top"] = ["Top 3" if k < 3 else "Resto" for k in range(len(cambio))]
            fig = px.bar(cambio, x="variacion", y="zona", orientation="h", color="top",
                         color_discrete_map={"Top 3": "#E25A1C", "Resto": "#ADB5BD"},
                         labels={"variacion": f"Variación de {contam} (%)", "zona": ""}, text_auto=".1f")
            fig.update_layout(height=290, margin=dict(l=0, r=0, t=10, b=0), legend_title_text="",
                              yaxis=dict(categoryorder="total ascending"))
            st.plotly_chart(fig, width="stretch")
            st.caption(f"Primer mes vs último mes del período, a partir de los indicadores mensuales que Spark "
                       f"calculó sobre 8,7 M de lecturas leídas desde HDFS (excluye anomalías).")

    # ---------------- mapa
    st.markdown("---")
    m1, m2 = st.columns([3, 2])
    with m1:
        st.markdown("**Mapa de sensores** (tamaño = anomalías detectadas por Spark en el período)")
        etiqueta("PostgreSQL", "Spark")
        conteo = anom_f.groupby("sensor").size().rename("anomalias")
        mapa = cat_f.merge(conteo, left_on="sensor", right_index=True, how="left").fillna({"anomalias": 0})
        mapa["tam"] = 6 + mapa["anomalias"].clip(upper=200) / 4
        mapa_nuevo = hasattr(px, "scatter_map")          # plotly >= 5.24
        graf_mapa = px.scatter_map if mapa_nuevo else px.scatter_mapbox
        fig = graf_mapa(mapa, lat="lat", lon="lon", color="zona", size="tam", size_max=22,
                        color_discrete_map=COLORES_ZONA, zoom=10.6, center={"lat": 56.165, "lon": 10.19},
                        hover_name="sensor", hover_data={"tipo": True, "estado": True, "anomalias": True,
                                                         "tam": False, "lat": False, "lon": False, "zona": False})
        if mapa_nuevo:
            fig.update_layout(map_style="open-street-map")
        else:
            fig.update_layout(mapbox_style="open-street-map")
        fig.update_layout(height=430, margin=dict(l=0, r=0, t=0, b=0), legend_title_text="",
                          legend=dict(orientation="h", yanchor="top", y=-0.02, x=0))
        st.plotly_chart(fig, width="stretch")
    with m2:
        st.markdown("**Sensores sin actividad** (últimos 7 días de datos)")
        etiqueta("PostgreSQL")
        sin_act = sql("SELECT * FROM fn_sensores_sin_actividad(7)")
        st.dataframe(sin_act[sin_act["zona"].isin(zonas)], hide_index=True, height=200, width="stretch")
        st.markdown("**Consumo eléctrico promedio por hora del día**")
        etiqueta("MongoDB")
        if consumo_hora.empty:
            st.info("MongoDB no disponible.")
        else:
            fig = px.bar(consumo_hora, x="hora", y="consumo", labels={"consumo": "kWh", "hora": "hora"})
            fig.update_layout(height=170, margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig, width="stretch")

    # ---------------- tabla de detalle
    st.markdown("---")
    st.subheader("Tabla de detalle de eventos")
    t1, t2, t3 = st.columns([2, 2, 3])
    tipo_evento = t1.selectbox("Tipo de evento", ["Anomalías (Spark)", "Alertas por umbral (MongoDB)"])
    if tipo_evento.startswith("Anomalías"):
        etiqueta("Spark", "PostgreSQL")
        tabla = anom_f[anom_f["tipo"].isin(tipos)]
        if sensor_sel != "Todos":
            tabla = tabla[tabla["sensor"] == sensor_sel]
        var_t = t2.selectbox("Variable", ["Todas"] + sorted(tabla["variable"].unique()))
        if var_t != "Todas":
            tabla = tabla[tabla["variable"] == var_t]
        if len(tabla):
            lo, hi = float(tabla["valor"].min()), float(tabla["valor"].max())
            if hi > lo:
                rango = t3.slider("Rango de valores", lo, hi, (lo, hi))
                tabla = tabla[tabla["valor"].between(*rango)]
        tabla = tabla.sort_values("puntaje", key=abs, ascending=False)
    else:
        etiqueta("MongoDB")
        var_t = t2.selectbox("Variable", list(LIMITES))
        tabla = pd.DataFrame()
        if estado["MongoDB"]:
            cond = {"mediciones." + var_t: {"$gt": LIMITES[var_t]}, "zona.id": {"$in": ids_zona},
                    "timestamp": rango_mongo(desde, hasta)}
            if sensor_sel != "Todos":
                cond["sensor_id"] = sensor_sel
            docs = mongo()["iot_readings"].find(cond, {"_id": 0, "sensor_id": 1, "zona.nombre": 1, "tipo": 1,
                                                        "timestamp": 1, "mediciones." + var_t: 1}).limit(2000)
            tabla = pd.DataFrame([{"timestamp": d["timestamp"], "sensor": d["sensor_id"], "zona": d["zona"]["nombre"],
                                   "tipo": d["tipo"], "variable": var_t, "valor": d["mediciones"][var_t],
                                   "limite": LIMITES[var_t]} for d in docs])
        t3.caption(f"Límite de alerta: {LIMITES[var_t]} (máx. 2 000 filas)")
    busqueda = st.text_input("Buscar (sensor, zona, variable…)", "")
    if busqueda and len(tabla):
        tabla = tabla[tabla.astype(str).apply(lambda r: r.str.contains(busqueda, case=False)).any(axis=1)]
    por_pagina = 25
    paginas = max(1, -(-len(tabla) // por_pagina))
    pagina = st.number_input(f"Página (de {paginas}) · {len(tabla):,} registros".replace(",", " "),
                             min_value=1, max_value=paginas, value=1)
    st.dataframe(tabla.iloc[(pagina - 1) * por_pagina: pagina * por_pagina], hide_index=True, width="stretch")


# ===================================================== consultas por tecnología
CONSULTAS_PG = {
    "1. Sensores por tipo": """
        SELECT c.nombre_categoria AS categoria, t.nombre AS tipo, COUNT(s.id_sensor) AS sensores
        FROM tipo_sensor t JOIN categoria c ON c.id_categoria = t.id_categoria
        JOIN sensor s ON s.id_tipo = t.id_tipo GROUP BY 1, 2 ORDER BY sensores DESC""",
    "2. Sensores por ubicación": """
        SELECT z.nombre AS zona, u.descripcion AS ubicacion, u.latitud, u.longitud, COUNT(s.id_sensor) AS sensores
        FROM ubicacion u JOIN zona z ON z.id_zona = u.id_zona LEFT JOIN sensor s ON s.id_ubicacion = u.id_ubicacion
        GROUP BY z.nombre, u.id_ubicacion ORDER BY z.nombre, u.descripcion""",
    "3. Sensores activos e inactivos": """
        SELECT estado, COUNT(*) AS sensores, ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS porcentaje
        FROM sensor GROUP BY estado ORDER BY sensores DESC""",
    "4. Sensores por organismo responsable": """
        SELECT o.nombre AS organismo, o.responsable, COUNT(s.id_sensor) AS total,
               COUNT(s.id_sensor) FILTER (WHERE s.estado = 'activo') AS activos
        FROM organismo o LEFT JOIN sensor s ON s.id_organismo = o.id_organismo GROUP BY o.id_organismo ORDER BY total DESC""",
    "5. Ubicaciones con mayor cantidad de sensores": """
        WITH por_ubicacion AS (
            SELECT u.id_ubicacion, z.nombre AS zona, u.descripcion, COUNT(s.id_sensor) AS sensores
            FROM ubicacion u JOIN zona z ON z.id_zona = u.id_zona LEFT JOIN sensor s ON s.id_ubicacion = u.id_ubicacion
            GROUP BY u.id_ubicacion, z.nombre)
        SELECT RANK() OVER (ORDER BY sensores DESC) AS posicion, zona, descripcion, sensores
        FROM por_ubicacion WHERE sensores > (SELECT AVG(sensores) FROM por_ubicacion) ORDER BY posicion""",
    "6. Sensores instalados por año": """
        SELECT EXTRACT(YEAR FROM fecha_instalacion)::INT AS anio, COUNT(*) AS instalados,
               SUM(COUNT(*)) OVER (ORDER BY EXTRACT(YEAR FROM fecha_instalacion)) AS acumulado
        FROM sensor GROUP BY EXTRACT(YEAR FROM fecha_instalacion) ORDER BY anio""",
    "7. Sensores sin actividad registrada": "SELECT * FROM fn_sensores_sin_actividad(7)",
    "8. Ranking de zonas monitoreadas": """
        WITH resumen AS (
            SELECT z.nombre AS zona, z.perfil, COUNT(s.id_sensor) AS sensores,
                   COUNT(s.id_sensor) FILTER (WHERE s.estado = 'activo') AS activos,
                   COUNT(DISTINCT u.id_ubicacion) AS ubicaciones
            FROM zona z LEFT JOIN ubicacion u ON u.id_zona = z.id_zona LEFT JOIN sensor s ON s.id_ubicacion = u.id_ubicacion
            GROUP BY z.id_zona, z.nombre, z.perfil)
        SELECT RANK() OVER (ORDER BY activos DESC) AS ranking, zona, perfil, sensores, activos, ubicaciones,
               ROUND(sensores::NUMERIC / NULLIF(ubicaciones, 0), 2) AS sensores_por_ubicacion
        FROM resumen ORDER BY ranking""",
    "9. Consulta utilizando vistas (mv_resumen_zona)": "SELECT * FROM mv_resumen_zona ORDER BY sensores DESC",
    "10. Consulta utilizando funciones (fn_sensores_por_zona)": "SELECT * FROM fn_sensores_por_zona(:zona)",
}


def consultas_mongo(nombre, desde, hasta, ids_zona, sensor):
    col = mongo()["iot_readings"]
    rango = rango_mongo(desde, hasta)
    zonas_f = {"zona.id": {"$in": ids_zona}}
    alerta = {"$or": [{"tipo": "calidad_aire", f"mediciones.{v}": {"$gt": LIMITES[v]}} for v in ("pm25", "pm10", "no2", "co", "co2")]
              + [{"tipo": "ruido", "mediciones.nivel_db": {"$gt": 70}},
                 {"tipo": "estacionamiento", "mediciones.ocupacion_pct": {"$gt": 95}},
                 {"tipo": "trafico", "mediciones.velocidad_promedio": {"$lt": 15}}]}
    if nombre.startswith("1."):
        s = sensor if sensor != "Todos" else "Z01-AIR-001"
        return pd.json_normalize(list(col.find({"sensor_id": s, "timestamp": rango},
                                               {"_id": 0, "sensor_id": 1, "timestamp": 1, "mediciones": 1})
                                      .sort("timestamp", 1).limit(200)))
    if nombre.startswith("2."):
        return pd.json_normalize(list(col.find({"timestamp": rango, **zonas_f},
                                               {"_id": 0, "sensor_id": 1, "tipo": 1, "zona.nombre": 1, "timestamp": 1})
                                      .limit(200)))
    if nombre.startswith("3."):
        return pd.DataFrame(list(col.aggregate([
            {"$match": {"timestamp": rango, **zonas_f}},
            {"$project": {"tipo": 1, "m": {"$objectToArray": "$mediciones"}}}, {"$unwind": "$m"},
            {"$group": {"_id": {"tipo": "$tipo", "variable": "$m.k"}, "minimo": {"$min": "$m.v"}, "maximo": {"$max": "$m.v"}}},
            {"$project": {"_id": 0, "tipo": "$_id.tipo", "variable": "$_id.variable", "minimo": 1, "maximo": 1}},
            {"$sort": {"tipo": 1, "variable": 1}}], allowDiskUse=True)))
    if nombre.startswith("4.") and "eventos" in mongo().list_collection_names():
        return pd.json_normalize(list(mongo()["eventos"].find(
            {"severidad": "critica", "zona.id": {"$in": ids_zona}, "timestamp": rango},
            {"_id": 0, "sensor_id": 1, "zona.nombre": 1, "timestamp": 1, "variable": 1,
             "valor": 1, "esperado": 1, "puntaje": 1}).sort("puntaje", -1).limit(200)))
    if nombre.startswith("4."):
        return pd.json_normalize(list(col.find({"timestamp": rango, **zonas_f, **alerta},
                                               {"_id": 0, "sensor_id": 1, "tipo": 1, "zona.nombre": 1, "timestamp": 1, "mediciones": 1})
                                      .sort("timestamp", -1).limit(200)))
    if nombre.startswith("5.") and "alertas" in mongo().list_collection_names():
        return pd.DataFrame(list(mongo()["alertas"].aggregate([
            {"$match": {"zona.id": {"$in": ids_zona}, "timestamp": rango}},
            {"$group": {"_id": {"zona": "$zona.nombre", "severidad": "$severidad"}, "alertas": {"$sum": 1}}},
            {"$project": {"_id": 0, "zona": "$_id.zona", "severidad": "$_id.severidad", "alertas": 1}},
            {"$sort": {"zona": 1, "alertas": -1}}])))
    if nombre.startswith("5."):
        return pd.DataFrame(list(col.aggregate([
            {"$match": {"timestamp": rango, **zonas_f, **alerta}},
            {"$group": {"_id": {"zona": "$zona.nombre", "tipo": "$tipo"}, "alertas": {"$sum": 1}}},
            {"$project": {"_id": 0, "zona": "$_id.zona", "tipo": "$_id.tipo", "alertas": 1}},
            {"$sort": {"alertas": -1}}])))
    if nombre.startswith("6."):
        return pd.DataFrame(list(col.aggregate([
            {"$match": {"timestamp": rango, **zonas_f}},
            {"$project": {"zona": "$zona.nombre", "m": {"$objectToArray": "$mediciones"}}}, {"$unwind": "$m"},
            {"$group": {"_id": {"zona": "$zona", "variable": "$m.k"}, "promedio": {"$avg": "$m.v"}, "lecturas": {"$sum": 1}}},
            {"$project": {"_id": 0, "zona": "$_id.zona", "variable": "$_id.variable", "promedio": "$promedio", "lecturas": 1}},
            {"$sort": {"variable": 1, "zona": 1}}], allowDiskUse=True)))
    if nombre.startswith("7."):
        return pd.DataFrame(list(col.aggregate([
            {"$match": {"timestamp": rango, **zonas_f}},
            {"$project": {"tipo": 1, "m": {"$objectToArray": "$mediciones"}}}, {"$unwind": "$m"},
            {"$group": {"_id": {"tipo": "$tipo", "variable": "$m.k"}, "promedio": {"$avg": "$m.v"}}},
            {"$project": {"_id": 0, "tipo": "$_id.tipo", "variable": "$_id.variable",
                          "promedio": "$promedio"}},
            {"$sort": {"tipo": 1, "variable": 1}}], allowDiskUse=True)))
    if nombre.startswith("8.") and "incidentes" in mongo().list_collection_names():
        return pd.DataFrame(list(mongo()["incidentes"].aggregate([
            {"$match": {"zona.id": {"$in": ids_zona}}},
            {"$group": {"_id": "$categoria", "incidentes": {"$sum": 1},
                        "abiertos": {"$sum": {"$cond": [{"$in": ["$estado", ["reportado", "en_atencion"]]}, 1, 0]}}}},
            {"$project": {"_id": 0, "categoria": "$_id", "incidentes": 1, "abiertos": 1}},
            {"$sort": {"incidentes": -1}}])))
    if nombre.startswith("8."):
        return pd.DataFrame(list(col.aggregate([
            {"$match": {"timestamp": rango, **zonas_f, **alerta}},
            {"$group": {"_id": "$categoria", "incidentes": {"$sum": 1}, "sensores": {"$addToSet": "$sensor_id"}}},
            {"$project": {"_id": 0, "categoria": "$_id", "incidentes": 1, "sensores_afectados": {"$size": "$sensores"}}},
            {"$sort": {"incidentes": -1}}])))
    if nombre.startswith("9."):
        return pd.DataFrame(list(col.aggregate([
            {"$match": {"timestamp": rango, "tipo": "calidad_aire", **zonas_f}},
            {"$group": {"_id": {"dia": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}}, "zona": "$zona.nombre"},
                        "pm25": {"$avg": "$mediciones.pm25"}, "no2": {"$avg": "$mediciones.no2"}}},
            {"$project": {"_id": 0, "dia": "$_id.dia", "zona": "$_id.zona",
                          "pm25": "$pm25", "no2": "$no2"}},
            {"$sort": {"dia": 1, "zona": 1}}], allowDiskUse=True)))
    if nombre.startswith("10."):
        return pd.json_normalize(list(col.find(
            {"tipo": "calidad_aire", "timestamp": rango, **zonas_f,
             "mediciones.pm25": {"$gt": 25}, "mediciones.no2": {"$gt": 30}},
            {"_id": 0, "sensor_id": 1, "zona.nombre": 1, "timestamp": 1, "mediciones.pm25": 1, "mediciones.no2": 1})
            .sort("mediciones.pm25", -1).limit(200)))


NOMBRES_MONGO = ["1. Lecturas de un sensor específico", "2. Lecturas en un rango de fechas",
                 "3. Valores máximos y mínimos registrados", "4. Eventos críticos detectados",
                 "5. Alertas emitidas por zona geográfica", "6. Promedio de mediciones por zona",
                 "7. Promedio por tipo de sensor", "8. Incidentes registrados por categoría",
                 "9. Consulta con agregaciones (evolución diaria de PM2.5 y NO₂)",
                 "10. Consulta con filtros múltiples (PM2.5 > 25 y NO₂ > 30)"]

HADOOP = {
    "Estado de HDFS (espacio por directorio)": "hdfs dfs -du -s -h /smartcity/*",
    "Bloques y réplicas (fsck)": "hdfs fsck /smartcity | grep -E 'Status|Total size|Total files|Total blocks|replication|Missing'",
    "Q1. ¿Cuántas mediciones existen por sensor?": "hdfs dfs -cat '/smartcity/results/q1_mediciones_por_sensor/part-*' | sort",
    "Q2. ¿Cuál sensor produjo mayor cantidad de datos?": "hdfs dfs -cat '/smartcity/results/q2_sensor_mas_datos/part-*'",
    "Q3. ¿Cuál es la temperatura promedio histórica?": "hdfs dfs -cat '/smartcity/results/q3_temperatura_promedio/part-*' | sort",
    "Q4. ¿Cuál es la cantidad de eventos por zona?": "hdfs dfs -cat '/smartcity/results/q4_eventos_por_zona/part-*' | sort",
    "Q5. Comportamiento durante un período": "for d in $(hdfs dfs -ls -C /smartcity/results | grep q5_); do echo \"## $d\"; hdfs dfs -cat \"$d/part-*\" | sort | head -40; done",
}
COLUMNAS_HADOOP = {"Q1": ["sensor", "mediciones"], "Q2": ["resultado", "sensor", "mediciones"],
                   "Q3": ["zona", "temperatura_promedio", "mediciones"], "Q4": ["zona|evento", "cantidad"]}

with pag_consultas:
    st.title("Consultas por tecnología")
    st.caption("Las consultas usan los filtros de la barra lateral (período, zona y sensor) cuando aplican.")
    tecnologia = st.radio("Tecnología", ["PostgreSQL", "MongoDB", "Hadoop", "Spark"], horizontal=True)

    if tecnologia == "PostgreSQL":
        etiqueta("PostgreSQL")
        nombre = st.selectbox("Consulta obligatoria", list(CONSULTAS_PG))
        params = {}
        if "fn_sensores_por_zona" in nombre:
            params["zona"] = st.selectbox("Zona", sorted(cat["id_zona"].unique()))
        st.code(CONSULTAS_PG[nombre].strip(), language="sql")
        st.dataframe(sql(CONSULTAS_PG[nombre], params), hide_index=True, width="stretch")

    elif tecnologia == "MongoDB":
        etiqueta("MongoDB")
        if not estado["MongoDB"]:
            st.error(f"No hay conexión con MongoDB ({MONGO_URI}).")
        else:
            nombre = st.selectbox("Consulta obligatoria", NOMBRES_MONGO)
            with st.spinner("Consultando MongoDB…"):
                res = consultas_mongo(nombre, desde, hasta, ids_zona, sensor_sel)
                numericas = res.select_dtypes("number").columns
                res[numericas] = res[numericas].round(2)
            st.caption(f"Colección iot_readings · {len(res):,} filas".replace(",", " "))
            if nombre.startswith("9.") and len(res):
                fig = px.line(res, x="dia", y="pm25", color="zona", color_discrete_map=COLORES_ZONA)
                fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
                st.plotly_chart(fig, width="stretch")
            st.dataframe(res, hide_index=True, width="stretch")

    elif tecnologia == "Hadoop":
        etiqueta("HDFS", "Hadoop MapReduce")
        if not estado["HDFS"]:
            st.error(f"No se pudo ejecutar comandos en el contenedor '{HDFS_CONTENEDOR}'.")
        else:
            nombre = st.selectbox("Consulta", list(HADOOP))
            st.code(HADOOP[nombre], language="bash")
            with st.spinner("Leyendo desde HDFS…"):
                lineas = hdfs(HADOOP[nombre])
            clave = nombre[:2]
            if clave in COLUMNAS_HADOOP and lineas:
                df = pd.DataFrame([l.split("\t") for l in lineas])
                df.columns = COLUMNAS_HADOOP[clave][:df.shape[1]] + [f"c{i}" for i in range(df.shape[1] - len(COLUMNAS_HADOOP[clave]))]
                st.dataframe(df, hide_index=True, width="stretch")
            else:
                st.text("\n".join(lineas) or "Sin resultados. Ejecuta primero hadoop/ejecutar_mapreduce.sh.")

    else:
        etiqueta("Spark", "HDFS", "PostgreSQL")
        opcion = st.selectbox("Resultado de Spark", [
            "Indicadores mensuales por zona (indicador_zona_mensual)",
            "Anomalías detectadas por zona y tipo",
            "Detalle de anomalías con mayor puntaje",
            "Resultados Parquet guardados en HDFS"])
        if opcion.startswith("Indicadores"):
            ind = indicadores_spark()
            v = st.selectbox("Variable", sorted(ind["variable"].unique()))
            d = ind[(ind["variable"] == v) & ind["zona"].isin(zonas)].copy()
            d["mes_txt"] = pd.to_datetime(dict(year=d["anio"], month=d["mes"], day=1))
            fig = px.line(d, x="mes_txt", y="promedio", color="zona", color_discrete_map=COLORES_ZONA,
                          labels={"mes_txt": "", "promedio": v}, markers=True)
            fig.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, width="stretch")
            st.dataframe(d.drop(columns=["mes_txt"]).sort_values(["anio", "mes", "zona"]), hide_index=True, width="stretch")
        elif opcion.startswith("Anomalías"):
            a = anomalias_spark()
            a = a[a["zona"].isin(zonas)].groupby(["zona", "tipo"]).size().reset_index(name="anomalias")
            fig = px.bar(a, x="zona", y="anomalias", color="tipo", barmode="stack")
            fig.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, width="stretch")
            st.dataframe(a, hide_index=True, width="stretch")
        elif opcion.startswith("Detalle"):
            a = anomalias_spark()
            st.dataframe(a[a["zona"].isin(zonas)].sort_values("puntaje", key=abs, ascending=False).head(500),
                         hide_index=True, width="stretch")
        else:
            if not estado["HDFS"]:
                st.error("HDFS no disponible.")
            else:
                st.text("\n".join(hdfs("hdfs dfs -ls -R /smartcity/results /smartcity/iot/results 2>/dev/null | grep -E 'parquet|^d' | head -60")))
