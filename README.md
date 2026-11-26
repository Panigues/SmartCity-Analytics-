# SmartCity Analytics

**Proyecto II – CE3101 Bases de Datos II, II Semestre 2026 (TEC).**

Arquitectura de datos para una ciudad inteligente simulada. El proyecto integra diferentes tecnologías para almacenar, procesar y analizar grandes volúmenes de información provenientes de sensores IoT.

## Arquitectura del proyecto

El proyecto utiliza las siguientes tecnologías:

| Tecnología | Uso en el proyecto |
|---|---|
| PostgreSQL 16 | Almacenamiento de datos maestros, lecturas agregadas, indicadores y anomalías detectadas. |
| MongoDB | Almacenamiento de lecturas IoT y datos semiestructurados. |
| HDFS (Hadoop) | Almacenamiento distribuido de datos RAW y procesados. |
| Apache Spark | Procesamiento distribuido de datos, transformación, agregación y detección de anomalías. |
| Python | Generación de datos simulados e integración entre tecnologías. |

### Flujo de procesamiento

El procesamiento de los datos IoT sigue esta arquitectura:

```text
Generador IoT
     |
     v
  MongoDB
     |
     v
    HDFS
  (Datos RAW)
     |
     v
   Spark
     |
     +----> HDFS (Datos procesados y resultados)
     |
     v
 PostgreSQL
     |
     v
  Dashboard
  (Pendiente)
```

PostgreSQL almacena los resultados estructurados que posteriormente utilizará el dashboard para presentar indicadores y visualizaciones.

## Fuentes de datos

El proyecto utiliza las siguientes fuentes:

- **Energía:** UCI Individual Household Electric Power Consumption.
- **Ambiente:** OpenAQ.
- **Movilidad:** CityPulse (Aarhus).

La ciudad simulada se divide en cinco zonas:

| Código | Zona |
|---|---|
| Z01 | Centro |
| Z02 | Norte |
| Z03 | Sur |
| Z04 | Este |
| Z05 | Oeste |

Los parámetros de la simulación se encuentran en `config/smartcity_config.json`.

## Estructura del repositorio

```text
config/          Parámetros de la simulación
data/raw/        Datasets descargados (no se suben a GitHub)
data/generated/  Datos generados (no se suben a GitHub)
generator/       Scripts de generación y carga de datos
postgres/        Scripts SQL, tablas, vistas y consultas
mongodb/         Scripts y consultas de MongoDB
hadoop/          Configuración y comandos de HDFS
spark/jobs/      Procesos ETL desarrollados con PySpark
spark/tests/     Pruebas y verificaciones de procesamiento
dashboard/       Interfaz de visualización (pendiente)
docs/            Documentación técnica y diagramas
```

## Requisitos

- Docker Desktop.
- Al menos 6 GB de RAM asignados a Docker (recomendado).
- Git.
- Python 3.10 o superior.
- MongoDB.
- Apache Spark 4.0.1.
- Java 17 para el entorno Spark.

Las dependencias adicionales de Python y los conectores de Spark se utilizan según el proceso que se vaya a ejecutar.

## Puesta en marcha

1. Clonar el repositorio y entrar en la carpeta del proyecto.
2. Crear el archivo de configuración local a partir de la plantilla, si está disponible:

```powershell
copy .env.example .env
```

3. Configurar las variables de entorno y las contraseñas.
4. Iniciar Docker Desktop y esperar a que el motor esté activo.
5. Levantar los servicios definidos en Docker Compose:

```bash
docker compose up -d
docker compose ps
```

**Importante:** Docker Compose contiene la infraestructura de PostgreSQL y Hadoop. MongoDB y el contenedor Spark utilizado en el procesamiento IoT se ejecutan de forma independiente en el entorno de desarrollo actual. Por lo tanto, levantar Docker Compose no reproduce automáticamente todo el pipeline.

## Servicios

| Servicio | Contenedor / entorno | Acceso |
|---|---|---|
| PostgreSQL 16 | `smartcity-postgres` | `localhost:5433` |
| HDFS NameNode | `namenode` | Interfaz web: http://localhost:9870 |
| HDFS DataNodes | `datanode1`, `datanode2` | Comunicación interna de Docker |
| MongoDB | Servicio local | `localhost:27017` |
| Apache Spark 4.0.1 | `spark-smartcity` | Contenedor Docker independiente |

### PostgreSQL

Base de datos: `smartcity`.

Conexión desde el contenedor:

```bash
docker exec -it smartcity-postgres psql -U smartcity -d smartcity
```

Desde pgAdmin o una aplicación externa:

| Parámetro | Valor |
|---|---|
| Host | `localhost` |
| Puerto | `5433` |
| Base de datos | `smartcity` |
| Usuario | `smartcity` |
| Contraseña | Configurada localmente |

La contraseña debe obtenerse de la configuración local y no debe publicarse en GitHub.

### MongoDB

Base de datos: `smartcity`.

Colección principal de lecturas IoT: `iot_readings`.

Conexión local:

```text
mongodb://localhost:27017
```

La colección principal contiene **2,906,713 documentos** correspondientes a las lecturas simuladas de los sensores.

También se incluyen consultas de ejemplo en:

```text
mongodb/scripts/consultas_mongodb.js
```

### HDFS

HDFS está compuesto por:

- 1 NameNode.
- 2 DataNodes.
- Factor de replicación: 2.
- Tamaño de bloque configurado: 32 MB.

Para consultar los archivos:

```bash
docker exec -it namenode hdfs dfs -ls /
```

Para verificar el estado del clúster:

```bash
docker exec -it namenode hdfs dfsadmin -report
```

Directorios principales del procesamiento IoT:

| Directorio | Contenido |
|---|---|
| `/smartcity/iot/raw` | Documentos originales provenientes de MongoDB. |
| `/smartcity/iot/processed` | Mediciones IoT transformadas mediante Spark. |
| `/smartcity/iot/results/spark_a_postgres` | Resultados analíticos generados por Spark. |

### Apache Spark

Versión utilizada: **4.0.1**.

El procesamiento se ejecuta en el contenedor `spark-smartcity`, utilizando los conectores correspondientes para MongoDB, PostgreSQL y HDFS.

Los procesos principales se encuentran en:

```text
spark/jobs/mongodb_iot_to_hdfs.py
spark/jobs/spark_a_postgres.py
```

## Resultados del procesamiento

Se completó la integración del pipeline IoT y la carga de resultados en PostgreSQL.

### Datos procesados

| Métrica | Resultado |
|---|---:|
| Sensores simulados | 168 |
| Documentos IoT en MongoDB | 2,906,713 |
| Documentos RAW almacenados en HDFS | 2,906,713 |
| Mediciones procesadas mediante Spark | 8,763,393 |
| Registros rechazados por el ETL | 0 |

### Resultados cargados en PostgreSQL

| Tabla | Registros |
|---|---:|
| `sensor` | 168 |
| `lectura_diaria` | 366,955 |
| `indicador_zona_mensual` | 2,034 |
| `anomalia_detectada` | 38,656 |

También se encuentran disponibles las vistas `v_sensor_detalle` y `v_anomalias`, además de la vista materializada `mv_resumen_zona`.

### Detección de anomalías

El proceso Spark utiliza un método estadístico basado en la mediana y la desviación absoluta mediana (MAD) para identificar posibles anomalías.

En la comparación con las anomalías inyectadas durante la generación de datos se obtuvo:

- Anomalías inyectadas: 3,102.
- Anomalías detectadas correctamente: 2,454.
- Recall: 79.11 %.

Este resultado representa la proporción de anomalías inyectadas que el proceso logró identificar.

## Comandos útiles

| Acción | Comando |
|---|---|
| Levantar servicios Compose | `docker compose up -d` |
| Ver estado de servicios | `docker compose ps` |
| Consultar logs | `docker compose logs <servicio>` |
| Apagar servicios | `docker compose down` |
| Consultar archivos HDFS | `docker exec -it namenode hdfs dfs -ls /` |
| Consultar estado HDFS | `docker exec -it namenode hdfs dfsadmin -report` |

**Advertencia:** `docker compose down -v` elimina los volúmenes asociados y puede ocasionar la pérdida de datos persistidos. No utilizarlo si se desea conservar la información cargada.

## Estado del proyecto

- [x] PostgreSQL en Docker.
- [x] Creación de tablas y estructuras SQL.
- [x] Carga de datos maestros en PostgreSQL.
- [x] Configuración de MongoDB.
- [x] Generación de datos IoT.
- [x] Importación de lecturas IoT en MongoDB.
- [x] Configuración de HDFS.
- [x] Integración MongoDB → HDFS mediante Spark.
- [x] Procesamiento y transformación de datos IoT.
- [x] Detección de anomalías mediante Spark.
- [x] Carga de resultados analíticos en PostgreSQL.
- [x] Validación de los registros procesados.
- [ ] Desarrollo e integración del dashboard.
- [ ] Validación integral del despliegue en un entorno externo.

## Herramientas adicionales

Se recomienda utilizar extensiones de Visual Studio Code para facilitar la administración de contenedores Docker y la conexión con bases de datos.
