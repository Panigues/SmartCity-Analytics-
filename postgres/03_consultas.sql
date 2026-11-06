-- =====================================================================
-- SmartCity Analytics - Consultas PostgreSQL
-- Parte A: 10 consultas obligatorias del enunciado
-- Parte B: consultas que demuestran las técnicas requeridas
-- Cada consulta indica qué técnica usa.
-- =====================================================================


-- =====================================================================
-- PARTE A - CONSULTAS OBLIGATORIAS
-- =====================================================================

-- 1. Sensores por tipo
-- Técnicas: INNER JOIN, GROUP BY, COUNT, ORDER BY
SELECT c.nombre_categoria AS categoria,
       t.nombre           AS tipo,
       COUNT(s.id_sensor) AS sensores
FROM tipo_sensor t
JOIN categoria c ON c.id_categoria = t.id_categoria
JOIN sensor    s ON s.id_tipo      = t.id_tipo
GROUP BY c.nombre_categoria, t.nombre
ORDER BY sensores DESC;


-- 2. Sensores por ubicación
-- Técnicas: LEFT JOIN (incluye ubicaciones sin sensores), GROUP BY
SELECT z.nombre           AS zona,
       u.descripcion      AS ubicacion,
       u.latitud,
       u.longitud,
       COUNT(s.id_sensor) AS sensores
FROM ubicacion u
JOIN zona        z ON z.id_zona      = u.id_zona
LEFT JOIN sensor s ON s.id_ubicacion = u.id_ubicacion
GROUP BY z.nombre, u.id_ubicacion
ORDER BY z.nombre, u.descripcion;


-- 3. Cantidad de sensores activos e inactivos
-- Técnicas: GROUP BY, función de ventana SUM() OVER () para el porcentaje
SELECT estado,
       COUNT(*)                                            AS sensores,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)  AS porcentaje
FROM sensor
GROUP BY estado
ORDER BY sensores DESC;


-- 4. Número de sensores por organismo responsable
-- Técnicas: LEFT JOIN, GROUP BY, COUNT con FILTER
SELECT o.nombre                                               AS organismo,
       o.responsable,
       COUNT(s.id_sensor)                                     AS total,
       COUNT(s.id_sensor) FILTER (WHERE s.estado = 'activo')  AS activos
FROM organismo o
LEFT JOIN sensor s ON s.id_organismo = o.id_organismo
GROUP BY o.id_organismo
ORDER BY total DESC;


-- 5. Ubicaciones con mayor cantidad de sensores
-- Técnicas: CTE, subconsulta escalar en HAVING, RANK()
-- Muestra las ubicaciones que superan el promedio de sensores por ubicación.
WITH por_ubicacion AS (
    SELECT u.id_ubicacion, z.nombre AS zona, u.descripcion,
           COUNT(s.id_sensor) AS sensores
    FROM ubicacion u
    JOIN zona        z ON z.id_zona      = u.id_zona
    LEFT JOIN sensor s ON s.id_ubicacion = u.id_ubicacion
    GROUP BY u.id_ubicacion, z.nombre
)
SELECT RANK() OVER (ORDER BY sensores DESC) AS posicion,
       zona, descripcion, sensores
FROM por_ubicacion
WHERE sensores > (SELECT AVG(sensores) FROM por_ubicacion)
ORDER BY posicion;


-- 6. Sensores instalados por año
-- Técnicas: consulta sobre fechas (EXTRACT), acumulado con SUM() OVER (ORDER BY)
SELECT EXTRACT(YEAR FROM fecha_instalacion)::INT          AS anio,
       COUNT(*)                                           AS instalados,
       SUM(COUNT(*)) OVER (ORDER BY EXTRACT(YEAR FROM fecha_instalacion))
                                                          AS acumulado
FROM sensor
GROUP BY EXTRACT(YEAR FROM fecha_instalacion)
ORDER BY anio;


-- 7. Sensores sin actividad registrada
-- Técnicas: subconsulta correlacionada con NOT EXISTS
-- (Mientras lectura_diaria esté vacía, devuelve todos los sensores.)
SELECT s.nombre AS sensor, t.nombre AS tipo, s.estado
FROM sensor s
JOIN tipo_sensor t ON t.id_tipo = s.id_tipo
WHERE NOT EXISTS (
    SELECT 1 FROM lectura_diaria l WHERE l.id_sensor = s.id_sensor
)
ORDER BY s.nombre;


-- 8. Ranking de zonas monitoreadas
-- Técnicas: CTE, RANK() por sensores activos, DENSE_RANK() por densidad
-- (sensores por ubicación)
WITH resumen AS (
    SELECT z.id_zona, z.nombre AS zona, z.perfil,
           COUNT(s.id_sensor)                                    AS sensores,
           COUNT(s.id_sensor) FILTER (WHERE s.estado = 'activo') AS activos,
           COUNT(DISTINCT u.id_ubicacion)                        AS ubicaciones,
           COUNT(DISTINCT s.id_tipo)                             AS tipos
    FROM zona z
    LEFT JOIN ubicacion u ON u.id_zona      = z.id_zona
    LEFT JOIN sensor    s ON s.id_ubicacion = u.id_ubicacion
    GROUP BY z.id_zona, z.nombre, z.perfil
)
SELECT RANK()       OVER (ORDER BY activos DESC)  AS ranking,
       zona, perfil, sensores, activos, ubicaciones, tipos,
       ROUND(sensores::NUMERIC / NULLIF(ubicaciones, 0), 2) AS sensores_por_ubicacion,
       DENSE_RANK() OVER (ORDER BY sensores::NUMERIC / NULLIF(ubicaciones, 0) DESC)
                                                  AS ranking_densidad
FROM resumen
ORDER BY ranking;


-- 9. Consulta utilizando vistas
-- 9a. Vista normal: sensores de Ambiente que no están activos
SELECT sensor, tipo, zona, ubicacion, estado
FROM v_sensor_detalle
WHERE categoria = 'Ambiente' AND estado <> 'activo'
ORDER BY zona, sensor;

-- 9b. Vista materializada: resumen precalculado por zona
SELECT * FROM mv_resumen_zona ORDER BY sensores DESC;


-- 10. Consulta utilizando procedimientos almacenados o funciones
-- 10a. Función que devuelve una tabla
SELECT * FROM fn_sensores_por_zona('Z03');
SELECT * FROM fn_sensores_por_zona('Z01', 'inactivo');

-- 10b. Función de sensores sin actividad (últimos 7 días)
SELECT * FROM fn_sensores_sin_actividad(7) LIMIT 10;

-- 10c. Procedimiento almacenado (se revierte para no alterar los datos)
BEGIN;
CALL sp_cambiar_estado_sensor('Z01-AIR-001', 'mantenimiento');
SELECT sensor, estado FROM v_sensor_detalle WHERE sensor = 'Z01-AIR-001';
ROLLBACK;


-- =====================================================================
-- PARTE B - TÉCNICAS ADICIONALES REQUERIDAS
-- =====================================================================

-- B1. Consulta básica: SELECT, WHERE, ORDER BY, DISTINCT
-- Perfiles distintos de las zonas que tienen sensores de estacionamiento
SELECT DISTINCT perfil
FROM v_sensor_detalle
WHERE tipo = 'estacionamiento'
ORDER BY perfil;


-- B2. GROUP BY + HAVING + MIN / MAX / AVG
-- Tipos de sensor presentes en al menos 4 zonas, con su instalación más
-- antigua y más reciente
SELECT tipo,
       COUNT(DISTINCT id_zona)                  AS zonas,
       MIN(fecha_instalacion)                   AS primera_instalacion,
       MAX(fecha_instalacion)                   AS ultima_instalacion,
       ROUND(AVG(CURRENT_DATE - fecha_instalacion) / 365.25, 1)
                                                AS antiguedad_promedio_anios
FROM v_sensor_detalle
GROUP BY tipo
HAVING COUNT(DISTINCT id_zona) >= 4
ORDER BY zonas DESC, tipo;


-- B3. Subconsulta en FROM + función de ventana por partición
-- El sensor más antiguo de cada zona (ROW_NUMBER por zona)
SELECT zona, sensor, tipo, fecha_instalacion
FROM (
    SELECT zona, sensor, tipo, fecha_instalacion,
           ROW_NUMBER() OVER (PARTITION BY zona ORDER BY fecha_instalacion) AS n
    FROM v_sensor_detalle
) t
WHERE n = 1
ORDER BY zona;


-- B4. Funciones de ventana LAG: crecimiento de instalaciones año a año
WITH por_anio AS (
    SELECT EXTRACT(YEAR FROM fecha_instalacion)::INT AS anio, COUNT(*) AS instalados
    FROM sensor
    GROUP BY 1
)
SELECT anio, instalados,
       LAG(instalados) OVER (ORDER BY anio)              AS anio_anterior,
       instalados - LAG(instalados) OVER (ORDER BY anio) AS diferencia
FROM por_anio
ORDER BY anio;


-- B5. Consulta sobre fechas: sensores instalados por trimestre en 2022
SELECT DATE_TRUNC('quarter', fecha_instalacion)::DATE AS trimestre,
       COUNT(*)                                       AS instalados
FROM sensor
WHERE fecha_instalacion BETWEEN DATE '2022-01-01' AND DATE '2022-12-31'
GROUP BY 1
ORDER BY 1;


-- B6. Consulta utilizando índices
-- EXPLAIN muestra el plan de ejecución. Con pocas filas PostgreSQL
-- prefiere leer la tabla completa (Seq Scan); se desactiva para la
-- demostración y se verá "Index Scan using idx_sensor_estado".
SET enable_seqscan = off;
EXPLAIN ANALYZE
SELECT nombre FROM sensor WHERE estado = 'inactivo';
RESET enable_seqscan;
