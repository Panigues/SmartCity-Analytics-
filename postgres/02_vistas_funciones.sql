-- =====================================================================
-- SmartCity Analytics - Vistas, vista materializada, funciones y
-- procedimiento almacenado (PostgreSQL)
-- Ejecutar después de 01_ddl.sql y de cargar los datos maestros.
-- =====================================================================

-- ---------------------------------------------------------------------
-- VISTA: detalle completo de cada sensor
-- Junta las 6 tablas del modelo en una sola "tabla virtual".
-- Justificación: el modelo en 3FN reparte la información en varias
-- tablas; la vista evita repetir los mismos JOIN en cada consulta y en
-- el dashboard. No guarda datos: se recalcula cada vez que se consulta.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_sensor_detalle AS
SELECT
    s.id_sensor,
    s.nombre             AS sensor,
    t.nombre             AS tipo,
    c.nombre_categoria   AS categoria,
    z.id_zona,
    z.nombre             AS zona,
    z.distrito,
    z.perfil,
    u.id_ubicacion,
    u.descripcion        AS ubicacion,
    u.latitud,
    u.longitud,
    o.nombre             AS organismo,
    s.fecha_instalacion,
    s.estado
FROM sensor s
JOIN tipo_sensor t ON t.id_tipo      = s.id_tipo
JOIN categoria   c ON c.id_categoria = t.id_categoria
JOIN ubicacion   u ON u.id_ubicacion = s.id_ubicacion
JOIN zona        z ON z.id_zona      = u.id_zona
JOIN organismo   o ON o.id_organismo = s.id_organismo;


-- ---------------------------------------------------------------------
-- VISTA MATERIALIZADA: resumen por zona
-- A diferencia de la vista normal, SÍ guarda el resultado en disco.
-- Justificación: es un resumen que el dashboard consulta seguido y que
-- cambia poco (solo cuando se instalan sensores o llegan agregados de
-- Spark). Se actualiza con: REFRESH MATERIALIZED VIEW mv_resumen_zona;
-- ---------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mv_resumen_zona;
CREATE MATERIALIZED VIEW mv_resumen_zona AS
SELECT
    z.id_zona,
    z.nombre                                            AS zona,
    z.perfil,
    COUNT(DISTINCT u.id_ubicacion)                      AS ubicaciones,
    COUNT(s.id_sensor)                                  AS sensores,
    COUNT(s.id_sensor) FILTER (WHERE s.estado = 'activo') AS activos,
    COUNT(DISTINCT s.id_tipo)                           AS tipos_distintos
FROM zona z
LEFT JOIN ubicacion u ON u.id_zona      = z.id_zona
LEFT JOIN sensor    s ON s.id_ubicacion = u.id_ubicacion
GROUP BY z.id_zona, z.nombre, z.perfil;

CREATE UNIQUE INDEX idx_mv_resumen_zona ON mv_resumen_zona (id_zona);


-- ---------------------------------------------------------------------
-- FUNCIÓN: sensores de una zona, opcionalmente filtrados por estado
-- Uso: SELECT * FROM fn_sensores_por_zona('Z01');
--      SELECT * FROM fn_sensores_por_zona('Z03', 'inactivo');
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_sensores_por_zona(
    p_zona   VARCHAR,
    p_estado VARCHAR DEFAULT NULL
)
RETURNS TABLE (sensor VARCHAR, tipo VARCHAR, ubicacion VARCHAR, estado VARCHAR)
LANGUAGE sql STABLE
AS $$
    SELECT v.sensor, v.tipo, v.ubicacion, v.estado
    FROM v_sensor_detalle v
    WHERE v.id_zona = p_zona
      AND (p_estado IS NULL OR v.estado = p_estado)
    ORDER BY v.tipo, v.sensor;
$$;


-- ---------------------------------------------------------------------
-- FUNCIÓN: sensores sin actividad en los últimos N días
-- Toma como "hoy" la fecha más reciente registrada en lectura_diaria
-- (los datos son simulados, así que no se usa CURRENT_DATE).
-- Un sensor sin ninguna lectura aparece con ultima_lectura = NULL.
-- Uso: SELECT * FROM fn_sensores_sin_actividad(7);
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_sensores_sin_actividad(p_dias INT DEFAULT 7)
RETURNS TABLE (sensor VARCHAR, zona VARCHAR, estado VARCHAR,
               ultima_lectura DATE, dias_sin_datos INT)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_hoy DATE;
BEGIN
    SELECT COALESCE(MAX(fecha), CURRENT_DATE) INTO v_hoy FROM lectura_diaria;

    RETURN QUERY
    SELECT s.nombre, z.nombre, s.estado, ult.ultima,
           (v_hoy - ult.ultima)::INT
    FROM sensor s
    JOIN ubicacion u ON u.id_ubicacion = s.id_ubicacion
    JOIN zona      z ON z.id_zona      = u.id_zona
    LEFT JOIN (
        SELECT id_sensor, MAX(fecha) AS ultima
        FROM lectura_diaria
        GROUP BY id_sensor
    ) ult ON ult.id_sensor = s.id_sensor
    WHERE ult.ultima IS NULL
       OR ult.ultima < v_hoy - p_dias
    ORDER BY ult.ultima NULLS FIRST, s.nombre;
END;
$$;


-- ---------------------------------------------------------------------
-- PROCEDIMIENTO ALMACENADO: cambiar el estado de un sensor
-- Valida que el sensor exista y lanza un error claro si no.
-- El CHECK de la tabla ya impide estados inválidos.
-- Uso: CALL sp_cambiar_estado_sensor('Z01-AIR-003', 'mantenimiento');
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sp_cambiar_estado_sensor(
    p_sensor VARCHAR,
    p_estado VARCHAR
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_anterior VARCHAR;
BEGIN
    SELECT estado INTO v_anterior FROM sensor WHERE nombre = p_sensor;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'El sensor % no existe', p_sensor;
    END IF;

    UPDATE sensor SET estado = p_estado WHERE nombre = p_sensor;
    RAISE NOTICE 'Sensor %: % -> %', p_sensor, v_anterior, p_estado;
END;
$$;
