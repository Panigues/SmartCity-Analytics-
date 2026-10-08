# SmartCity-Analytics-


# SmartCity Analytics

Proyecto II – CE3101 Bases de Datos, II Semestre 2026 (TEC).

Arquitectura de datos para una ciudad inteligente simulada. Combina varias
tecnologías para demostrar que no existe una única tecnología adecuada para
todos los tipos de datos:

| Tecnología | Uso en el proyecto |
|---|---|
| PostgreSQL | Datos maestros y agregados estructurados |
| MongoDB | Lecturas, eventos y alertas semi-estructurados |
| HDFS (Hadoop) | Almacenamiento distribuido de grandes volúmenes |
| Spark | Procesamiento batch y near-real-time |

## Fuentes de datos

- **Energía:** UCI Individual Household Electric Power Consumption
- **Ambiente:** OpenAQ
- **Movilidad:** CityPulse (Aarhus)

La ciudad simulada se divide en 5 zonas (Z01 Centro, Z02 Norte, Z03 Sur,
Z04 Este, Z05 Oeste). Sus parámetros están en `config/smartcity_config.json`.

## Estructura del repositorio

```
config/          Parámetros de la simulación (zonas, sensores, umbrales)
data/raw/        Datasets descargados (no se suben a GitHub)
data/generated/  Datos generados por la simulación (no se suben)
generator/       Scripts de simulación de sensores IoT
postgres/        Scripts SQL (DDL, carga, consultas)
mongodb/         Scripts de MongoDB
hadoop/          Configuración y comandos de HDFS
spark/jobs/      Jobs de PySpark
dashboard/       Interfaz de visualización
docs/            Documento técnico y diagramas
```

## Requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) con al
  menos 6 GB de RAM asignados (Settings → Resources)
- Git
- Python 3.10+ (para el generador)

## Puesta en marcha

1. Clonar el repositorio y entrar a la carpeta.
2. Crear el archivo de contraseñas a partir de la plantilla:
```powershell
   copy .env.example .env      # Windows
   cp .env.example .env        # Linux / Mac
```
   Editar `.env` y cambiar las contraseñas.
3. Abrir Docker Desktop y esperar a que diga *Engine running*.
4. Levantar los servicios:
```bash
   docker compose up -d
   docker compose ps
```
   Todos los contenedores deben aparecer en estado *Up*.

## Servicios

| Servicio | Contenedor | Acceso |
|---|---|---|
| PostgreSQL 16 | `smartcity-postgres` | `localhost:5433` |
| HDFS NameNode | `namenode` | UI web: http://localhost:9870 |
| HDFS DataNodes | `datanode1`, `datanode2` | internos |

> PostgreSQL usa el puerto **5433** en la máquina local para no chocar con
> una instalación local de PostgreSQL (que usa 5432).

### Conectarse a PostgreSQL

```bash
docker exec -it smartcity-postgres psql -U smartcity -d smartcity
```

Desde pgAdmin: host `localhost`, puerto `5433`, usuario y contraseña del `.env`.

### Usar HDFS

```bash
docker exec -it namenode bash
hdfs dfs -ls /
```

Configuración del clúster (`hadoop/hadoop.env`):
- 1 NameNode y 2 DataNodes
- Replicación 2 (cada bloque se guarda en ambos DataNodes)
- Bloques de 32 MB (para que la distribución sea visible con archivos medianos)

Verificar que los dos DataNodes estén activos:
```bash
docker exec -it namenode hdfs dfsadmin -report
```

## Comandos útiles

| Acción | Comando |
|---|---|
| Levantar todo | `docker compose up -d` |
| Ver estado | `docker compose ps` |
| Ver logs de un servicio | `docker compose logs <servicio>` |
| Apagar (conserva datos) | `docker compose down` |
| Apagar y **borrar todos los datos** | `docker compose down -v` |

## Problemas comunes

- **`no configuration file provided`**: no estás en la carpeta del repo.
- **`variable is not set`**: falta el archivo `.env` o no está guardado.
- **`failed to connect to the docker API`**: Docker Desktop no está abierto.
- **Puerto ocupado**: otro programa usa ese puerto; cambiar el puerto
  izquierdo en `docker-compose.yml` (ej. `"5434:5432"`).
- Las variables del `.env` solo se aplican la primera vez. Si se cambian
  después, hay que recrear con `docker compose down -v`.

## Estado

- [x] PostgreSQL en Docker
- [x] HDFS (1 NameNode + 2 DataNodes)
- [ ] MongoDB
- [ ] Spark
- [ ] Generador de datos IoT
- [ ] Modelo y carga en PostgreSQL
- [ ] Carga en HDFS
- [ ] Dashboard

Como nota, se puede descargar un add on al vs code para que podas ver los containers mas graficamente