-- =====================================================================
-- SmartCity Analytics - Tablas que recibe PostgreSQL desde Spark
-- Ejecutar después de 01_ddl.sql (y de cargar los datos maestros).
--
-- Integración del enunciado: "datos limpios de Spark -> PostgreSQL".
-- Spark lee las lecturas desde HDFS, las limpia, detecta anomalías y
-- escribe aquí los resultados por JDBC. lectura_diaria (de 01_ddl.sql)
-- también la reescribe Spark con los datos ya limpios.
-- =====================================================================

DROP TABLE IF EXISTS indicador_zona_mensual, anomalia_detectada CASCADE;

-- Indicadores urbanos por zona, mes y variable (para el dashboard)
CREATE TABLE indicador_zona_mensual (
    id_zona   VARCHAR(3)     NOT NULL REFERENCES zona (id_zona),
    anio      INT            NOT NULL,
    mes       INT            NOT NULL,
    variable  VARCHAR(30)    NOT NULL,
    promedio  DECIMAL(12,4)  NOT NULL,
    maximo    DECIMAL(12,4)  NOT NULL,
    lecturas  INT            NOT NULL,
    anomalias INT            NOT NULL DEFAULT 0,
    PRIMARY KEY (id_zona, anio, mes, variable),
    CONSTRAINT chk_indicador_mes      CHECK (mes BETWEEN 1 AND 12),
    CONSTRAINT chk_indicador_lecturas CHECK (lecturas >= 0 AND anomalias >= 0)
);

-- Lecturas anómalas detectadas por Spark (valores fuera del comportamiento
-- normal del sensor a esa hora del día)
CREATE TABLE anomalia_detectada (
    id_sensor  INT            NOT NULL REFERENCES sensor (id_sensor) ON DELETE CASCADE,
    fecha_hora TIMESTAMP      NOT NULL,
    variable   VARCHAR(30)    NOT NULL,
    valor      DECIMAL(12,4)  NOT NULL,
    esperado   DECIMAL(12,4)  NOT NULL,   -- mediana del sensor a esa hora
    puntaje    DECIMAL(10,2)  NOT NULL,   -- desviación robusta (z basado en la mediana)
    PRIMARY KEY (id_sensor, fecha_hora, variable)
);

CREATE INDEX idx_anomalia_fecha ON anomalia_detectada (fecha_hora);
CREATE INDEX idx_indicador_periodo ON indicador_zona_mensual (anio, mes);

-- Vista para el dashboard: anomalías con el contexto del sensor
CREATE OR REPLACE VIEW v_anomalias AS
SELECT a.fecha_hora, d.sensor, d.tipo, d.zona, d.id_zona, a.variable,
       a.valor, a.esperado, a.puntaje
FROM anomalia_detectada a
JOIN v_sensor_detalle d ON d.id_sensor = a.id_sensor;
