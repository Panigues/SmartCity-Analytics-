
-- SmartCity Analytics - DDL PostgreSQ
DROP TABLE IF EXISTS lectura_diaria, sensor, usuario, ubicacion,
                     tipo_sensor, organismo, zona, categoria CASCADE;

-- ---------------------------------------------------------------------
-- 1. Tablas independientes (no llaves foraneas)
-- ---------------------------------------------------------------------

CREATE TABLE categoria (
    id_categoria     SERIAL       PRIMARY KEY,
    nombre_categoria VARCHAR(50)  NOT NULL UNIQUE
);

CREATE TABLE zona (
    id_zona  VARCHAR(3)   PRIMARY KEY,          
    nombre   VARCHAR(50)  NOT NULL UNIQUE,
    distrito VARCHAR(50)  NOT NULL,
    perfil   VARCHAR(40)  NOT NULL,
    CONSTRAINT chk_zona_codigo CHECK (id_zona ~ '^Z[0-9]{2}$')
);

CREATE TABLE organismo (
    id_organismo SERIAL        PRIMARY KEY,
    nombre       VARCHAR(100)  NOT NULL UNIQUE,
    responsable  VARCHAR(100)  NOT NULL
);

-- ---------------------------------------------------------------------
-- 2. Tablas con dependencias (llaves foráneas)
-- ---------------------------------------------------------------------

CREATE TABLE tipo_sensor (
    id_tipo      SERIAL       PRIMARY KEY,
    nombre       VARCHAR(50)  NOT NULL UNIQUE,
    id_categoria INT          NOT NULL REFERENCES categoria (id_categoria)
);

CREATE TABLE ubicacion (
    id_ubicacion SERIAL         PRIMARY KEY,
    id_zona      VARCHAR(3)     NOT NULL REFERENCES zona (id_zona),
    descripcion  VARCHAR(150),
    latitud      DECIMAL(10,8)  NOT NULL,
    longitud     DECIMAL(11,8)  NOT NULL,
    CONSTRAINT chk_ubicacion_lat CHECK (latitud  BETWEEN -90  AND 90),
    CONSTRAINT chk_ubicacion_lon CHECK (longitud BETWEEN -180 AND 180)
);

CREATE TABLE usuario (
    id_usuario   SERIAL        PRIMARY KEY,
    nombre       VARCHAR(100)  NOT NULL,
    correo       VARCHAR(150)  NOT NULL UNIQUE,
    id_organismo INT           REFERENCES organismo (id_organismo),  -- NULL = administrador
    CONSTRAINT chk_usuario_correo CHECK (correo LIKE '%_@_%._%')
);

-- ---------------------------------------------------------------------
-- 3. Sensor (depende de tipo, ubicación y organismo)
-- ---------------------------------------------------------------------

CREATE TABLE sensor (
    id_sensor         SERIAL        PRIMARY KEY,
    nombre            VARCHAR(100)  NOT NULL,
    id_tipo           INT           NOT NULL REFERENCES tipo_sensor (id_tipo),
    id_ubicacion      INT           NOT NULL REFERENCES ubicacion (id_ubicacion),
    id_organismo      INT           NOT NULL REFERENCES organismo (id_organismo),
    fecha_instalacion DATE          NOT NULL,
    estado            VARCHAR(15)   NOT NULL DEFAULT 'activo',
    CONSTRAINT chk_sensor_estado
        CHECK (estado IN ('activo', 'inactivo', 'mantenimiento'))
);

-- ---------------------------------------------------------------------
-- 4. Agregados diarios producidos por Spark
-- ---------------------------------------------------------------------

CREATE TABLE lectura_diaria (
    id_sensor INT            NOT NULL REFERENCES sensor (id_sensor) ON DELETE CASCADE,
    fecha     DATE           NOT NULL,
    variable  VARCHAR(30)    NOT NULL,
    promedio  DECIMAL(12,4),
    minimo    DECIMAL(12,4),
    maximo    DECIMAL(12,4),
    cantidad  INT            NOT NULL,
    PRIMARY KEY (id_sensor, fecha, variable),
    CONSTRAINT chk_lectura_cantidad CHECK (cantidad BETWEEN 0 AND 24),
    CONSTRAINT chk_lectura_rango    CHECK (minimo <= promedio AND promedio <= maximo)
);

-- ---------------------------------------------------------------------
-- 5. Índices
-- PostgreSQL indexa automáticamente las PK y las columnas UNIQUE,
-- pero NO las llaves foráneas. Se indexan las columnas usadas en
-- JOIN, WHERE y GROUP BY de las consultas obligatorias.
-- ---------------------------------------------------------------------

CREATE INDEX idx_tipo_sensor_categoria ON tipo_sensor (id_categoria);
CREATE INDEX idx_ubicacion_zona        ON ubicacion (id_zona);
CREATE INDEX idx_usuario_organismo     ON usuario (id_organismo);

CREATE INDEX idx_sensor_tipo           ON sensor (id_tipo);
CREATE INDEX idx_sensor_ubicacion      ON sensor (id_ubicacion);
CREATE INDEX idx_sensor_organismo      ON sensor (id_organismo);
CREATE INDEX idx_sensor_estado         ON sensor (estado);

CREATE INDEX idx_lectura_fecha         ON lectura_diaria (fecha);