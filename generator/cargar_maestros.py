"""
Carga los datos maestros de SmartCity en PostgreSQL a partir de
config/smartcity_config.json:

    categoria, organismo, zona, tipo_sensor, ubicacion, usuario, sensor

Uso (desde la raíz del repo):
    python generator/cargar_maestros.py            # carga en PostgreSQL
    python generator/cargar_maestros.py --dry-run  # solo muestra lo que cargaría

El script es re-ejecutable: vacía las tablas antes de insertar.
Con la misma semilla del JSON, siempre genera los mismos sensores.
"""
import argparse
import json
import math
import random
from datetime import date, timedelta
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
CONFIG = RAIZ / "config" / "smartcity_config.json"
ENV = RAIZ / ".env"

# Abreviaturas para los nombres de sensor (ej. Z01-AIR-003)
ABREV = {
    "energia": "ENE",
    "calidad_aire": "AIR",
    "clima": "CLI",
    "ruido": "RUI",
    "trafico": "TRA",
    "estacionamiento": "EST",
}

# Ubicaciones (puntos de instalación) por zona: más sensores -> más puntos
SENSORES_POR_UBICACION = 4

# Rango de fechas de instalación (antes del inicio de la simulación)
INSTALACION_DESDE = date(2019, 1, 1)
INSTALACION_HASTA = date(2023, 12, 31)


# ----------------------------------------------------------------- utilidades
def leer_env(ruta):
    """Lee un archivo .env simple (CLAVE=valor) sin dependencias externas."""
    valores = {}
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            clave, valor = linea.split("=", 1)
            valores[clave.strip()] = valor.strip()
    return valores


def punto_en_zona(centro, geo, rng):
    """
    Devuelve (lat, lon) aleatorio dentro de la zona:
      - 'radio':  círculo alrededor del centro de la ciudad (Centro).
      - 'sector': entre dos rumbos (0 = norte, 90 = este), fuera del centro.
    """
    if geo["tipo"] == "radio":
        rumbo = rng.uniform(0, 360)
        distancia_km = geo["radio_km"] * math.sqrt(rng.random())  # uniforme en área
    else:
        desde, hasta = geo["rumbo_desde"], geo["rumbo_hasta"]
        ancho = (hasta - desde) % 360  # maneja el norte: 315 -> 45 = 90 grados
        rumbo = (desde + rng.uniform(0, ancho)) % 360
        distancia_km = rng.uniform(1.6, 6.0)

    rad = math.radians(rumbo)
    dlat = distancia_km * math.cos(rad) / 111.32
    dlon = distancia_km * math.sin(rad) / (111.32 * math.cos(math.radians(centro["lat"])))
    return round(centro["lat"] + dlat, 8), round(centro["lon"] + dlon, 8)


def fecha_aleatoria(rng):
    dias = (INSTALACION_HASTA - INSTALACION_DESDE).days
    return INSTALACION_DESDE + timedelta(days=rng.randint(0, dias))


# ------------------------------------------------------------ construir filas
def construir_datos(cfg):
    rng = random.Random(cfg["simulacion"]["semilla"])
    nombres_cat = {c["id_categoria"]: c["nombre_categoria"] for c in cfg["categorias"]}

    categorias = [c["nombre_categoria"] for c in cfg["categorias"]]

    organismos = [
        (o["nombre"], o.get("responsable", "Por asignar")) for o in cfg["organismos"]
    ]
    # categoría -> organismo responsable
    organismo_de_cat = {}
    for o in cfg["organismos"]:
        for cat in o["categorias"]:
            organismo_de_cat[nombres_cat[cat]] = o["nombre"]

    zonas = [
        (z["zone_id"], z["nombre"], z["distrito"], z["perfil"]) for z in cfg["zonas"]
    ]

    tipos = [
        (nombre, nombres_cat[t["id_categoria"]])
        for nombre, t in cfg["tipos_sensor"].items()
    ]
    cat_de_tipo = dict(tipos)

    # Ubicaciones y sensores, zona por zona
    ubicaciones = []  # (codigo, id_zona, descripcion, lat, lon)
    sensores = []     # (nombre, tipo, codigo_ubicacion, organismo, fecha, estado)
    pct_inactivo = cfg["ruido_simulacion"]["sensores_inactivos_pct"] / 100
    pct_mantenimiento = 0.03

    for z in cfg["zonas"]:
        total = sum(z["sensores"].values())
        n_ubic = max(1, math.ceil(total / SENSORES_POR_UBICACION))
        codigos = []
        for i in range(1, n_ubic + 1):
            codigo = f"{z['zone_id']}-U{i:02d}"
            lat, lon = punto_en_zona(cfg["ciudad"]["centro"], z["asignacion_geo"], rng)
            ubicaciones.append((codigo, z["zone_id"], f"Punto {i:02d} - {z['nombre']}", lat, lon))
            codigos.append(codigo)

        for tipo, cantidad in z["sensores"].items():
            for n in range(1, cantidad + 1):
                azar = rng.random()
                if azar < pct_inactivo:
                    estado = "inactivo"
                elif azar < pct_inactivo + pct_mantenimiento:
                    estado = "mantenimiento"
                else:
                    estado = "activo"
                sensores.append((
                    f"{z['zone_id']}-{ABREV[tipo]}-{n:03d}",
                    tipo,
                    rng.choice(codigos),
                    organismo_de_cat[cat_de_tipo[tipo]],
                    fecha_aleatoria(rng),
                    estado,
                ))

    # Usuarios: 2 por organismo + 1 administrador sin organismo
    usuarios = [("Administrador del Sistema", "admin@smartcity.go.cr", None)]
    for i, o in enumerate(cfg["organismos"], start=1):
        for j in (1, 2):
            usuarios.append((
                f"Analista {j} - {o['nombre']}",
                f"analista{j}.org{i:02d}@smartcity.go.cr",
                o["nombre"],
            ))

    return {
        "categorias": categorias,
        "organismos": organismos,
        "zonas": zonas,
        "tipos": tipos,
        "ubicaciones": ubicaciones,
        "sensores": sensores,
        "usuarios": usuarios,
    }


# -------------------------------------------------------------- cargar en BD
def cargar(datos, env):
    import psycopg2
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(
        host="localhost",
        port=env.get("POSTGRES_PORT", "5433"),
        dbname=env["POSTGRES_DB"],
        user=env["POSTGRES_USER"],
        password=env["POSTGRES_PASSWORD"],
    )
    # 'with conn' = una sola transacción: si algo falla, no queda nada a medias
    with conn, conn.cursor() as cur:
        cur.execute("""
            TRUNCATE lectura_diaria, sensor, usuario, ubicacion,
                     tipo_sensor, organismo, zona, categoria
            RESTART IDENTITY CASCADE
        """)

        execute_values(cur, "INSERT INTO categoria (nombre_categoria) VALUES %s",
                       [(c,) for c in datos["categorias"]])
        execute_values(cur, "INSERT INTO organismo (nombre, responsable) VALUES %s",
                       datos["organismos"])
        execute_values(cur, "INSERT INTO zona (id_zona, nombre, distrito, perfil) VALUES %s",
                       datos["zonas"])

        # Diccionarios nombre -> id generado por SERIAL
        cur.execute("SELECT nombre_categoria, id_categoria FROM categoria")
        id_cat = dict(cur.fetchall())
        cur.execute("SELECT nombre, id_organismo FROM organismo")
        id_org = dict(cur.fetchall())

        execute_values(cur, "INSERT INTO tipo_sensor (nombre, id_categoria) VALUES %s",
                       [(t, id_cat[c]) for t, c in datos["tipos"]])
        cur.execute("SELECT nombre, id_tipo FROM tipo_sensor")
        id_tipo = dict(cur.fetchall())

        # Ubicaciones: RETURNING devuelve los id en el mismo orden de inserción
        filas = execute_values(
            cur,
            "INSERT INTO ubicacion (id_zona, descripcion, latitud, longitud) "
            "VALUES %s RETURNING id_ubicacion",
            [(z, d, la, lo) for _, z, d, la, lo in datos["ubicaciones"]],
            fetch=True,
        )
        id_ubic = {u[0]: f[0] for u, f in zip(datos["ubicaciones"], filas)}

        execute_values(cur, "INSERT INTO usuario (nombre, correo, id_organismo) VALUES %s",
                       [(n, c, id_org[o] if o else None) for n, c, o in datos["usuarios"]])

        execute_values(
            cur,
            "INSERT INTO sensor (nombre, id_tipo, id_ubicacion, id_organismo, "
            "fecha_instalacion, estado) VALUES %s",
            [(n, id_tipo[t], id_ubic[u], id_org[o], f, e)
             for n, t, u, o, f, e in datos["sensores"]],
        )
    conn.close()


# --------------------------------------------------------------------- main
def main():
    parser = argparse.ArgumentParser(description="Carga datos maestros en PostgreSQL")
    parser.add_argument("--dry-run", action="store_true",
                        help="No se conecta a la BD; solo muestra un resumen")
    args = parser.parse_args()

    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    datos = construir_datos(cfg)

    print("Filas a cargar:")
    for tabla, filas in datos.items():
        print(f"  {tabla:<12} {len(filas):>5}")

    if args.dry_run:
        print("\nEjemplo de sensores:")
        for s in datos["sensores"][:5]:
            print("  ", s)
        return

    cargar(datos, leer_env(ENV))
    print("\nCarga completada.")


if __name__ == "__main__":
    main()
