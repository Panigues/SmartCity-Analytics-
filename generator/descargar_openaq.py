"""
Descarga las mediciones de una estación de OpenAQ desde su archivo público
en AWS (no requiere cuenta de AWS ni API key).

Estructura del archivo de OpenAQ:
  records/csv.gz/locationid=<ID>/year=<AAAA>/month=<MM>/location-<ID>-<AAAAMMDD>.csv.gz

Uso (desde la raíz del repo):
    python generator/descargar_openaq.py --location 12345 --year 2024
    python generator/descargar_openaq.py --location 12345 --year 2024 --months 1 2 3

Los archivos quedan en data/raw/openaq/<ID>/ y al final se unen en
data/raw/openaq/openaq_<ID>_<AAAA>.csv
"""
import argparse
import gzip
import io
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

BUCKET = "https://openaq-data-archive.s3.amazonaws.com"
NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
RAIZ = Path(__file__).resolve().parent.parent


def listar_archivos(location, year, month):
    """Lista las claves del bucket para una estación, año y mes."""
    prefijo = f"records/csv.gz/locationid={location}/year={year}/month={month:02d}/"
    claves, token = [], None
    while True:
        params = {"list-type": "2", "prefix": prefijo}
        if token:
            params["continuation-token"] = token
        r = requests.get(BUCKET, params=params, timeout=60)
        r.raise_for_status()
        raiz = ET.fromstring(r.content)
        claves += [c.find("s3:Key", NS).text for c in raiz.findall("s3:Contents", NS)]
        if raiz.findtext("s3:IsTruncated", namespaces=NS) != "true":
            return claves
        token = raiz.findtext("s3:NextContinuationToken", namespaces=NS)


def main():
    p = argparse.ArgumentParser(description="Descarga datos de una estación OpenAQ")
    p.add_argument("--location", type=int, required=True, help="ID de la estación en OpenAQ")
    p.add_argument("--year", type=int, default=2024)
    p.add_argument("--months", type=int, nargs="*", default=list(range(1, 13)))
    args = p.parse_args()

    destino = RAIZ / "data" / "raw" / "openaq" / str(args.location)
    destino.mkdir(parents=True, exist_ok=True)

    descargados = 0
    for mes in args.months:
        claves = listar_archivos(args.location, args.year, mes)
        print(f"{args.year}-{mes:02d}: {len(claves)} archivos")
        for clave in claves:
            archivo = destino / Path(clave).name
            if archivo.exists():
                continue  # permite reanudar si se corta la descarga
            r = requests.get(f"{BUCKET}/{clave}", timeout=60)
            r.raise_for_status()
            archivo.write_bytes(r.content)
            descargados += 1

    print(f"Descargados {descargados} archivos nuevos en {destino}")

    # Unir todos los días en un solo CSV (se conserva el encabezado una vez)
    salida = destino.parent / f"openaq_{args.location}_{args.year}.csv"
    archivos = sorted(destino.glob(f"location-{args.location}-{args.year}*.csv.gz"))
    if not archivos:
        print("No hay datos para esa estación y año. Revisa el ID o prueba otro año.")
        return
    with open(salida, "w", encoding="utf-8", newline="") as out:
        for i, a in enumerate(archivos):
            with gzip.open(a, "rt", encoding="utf-8") as f:
                lineas = f.readlines()
            out.writelines(lineas if i == 0 else lineas[1:])
    print(f"CSV unido: {salida}")


if __name__ == "__main__":
    main()
