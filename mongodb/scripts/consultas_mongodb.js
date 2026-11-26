// ============================================================
// SMARTCITY ANALYTICS - CONSULTAS MONGODB
// Base de datos: smartcity
// Colecciones: iot_readings, pollution_measurements
// ============================================================

print("\n========== 1. CRUD: OPERACIONES DE DEMOSTRACIÓN ==========");

// INSERT ONE (ejemplo; no ejecutar sobre datos reales)
/*
db.iot_readings.insertOne({
    sensor_id: "Z01-TEST-001",
    tipo: "ruido",
    categoria: "ruido",
    zona: { id: "Z01", nombre: "Centro" },
    timestamp: new Date(),
    mediciones: { nivel_db: 55 }
});
*/

// INSERT MANY (ejemplo; no ejecutar sobre datos reales)
/*
db.iot_readings.insertMany([
    {
        sensor_id: "Z01-TEST-002",
        tipo: "ruido",
        zona: { id: "Z01", nombre: "Centro" },
        timestamp: new Date(),
        mediciones: { nivel_db: 60 }
    },
    {
        sensor_id: "Z02-TEST-003",
        tipo: "ruido",
        zona: { id: "Z02", nombre: "Norte" },
        timestamp: new Date(),
        mediciones: { nivel_db: 65 }
    }
]);
*/

// FIND: todas las lecturas de un sensor
print("\n--- Lecturas de un sensor ---");
db.iot_readings.find(
    { sensor_id: "Z01-AIR-001" },
    { _id: 0, sensor_id: 1, timestamp: 1, mediciones: 1 }
).limit(10).forEach(printjson);

// FIND ONE: una lectura de ejemplo
print("\n--- Una lectura de calidad del aire ---");
printjson(
    db.iot_readings.findOne(
        { tipo: "calidad_aire" },
        { _id: 0, sensor_id: 1, zona: 1, timestamp: 1, mediciones: 1 }
    )
);

// UPDATE ONE (ejemplo; no ejecutar sobre datos reales)
/*
db.iot_readings.updateOne(
    { sensor_id: "Z01-TEST-001" },
    { $set: { "mediciones.nivel_db": 58 } }
);
*/

// UPDATE MANY (ejemplo; no ejecutar sobre datos reales)
/*
db.iot_readings.updateMany(
    { sensor_id: /^Z01-TEST-/ },
    { $set: { categoria: "ruido_actualizado" } }
);
*/

// DELETE ONE (ejemplo; no ejecutar sobre datos reales)
/*
db.iot_readings.deleteOne({ sensor_id: "Z01-TEST-001" });
*/


print("\n========== 2. FILTROS Y CONSULTAS ==========");

// Lecturas de una zona
print("\n--- Lecturas de la zona Z01 ---");
db.iot_readings.find(
    { "zona.id": "Z01" },
    { _id: 0, sensor_id: 1, tipo: 1, timestamp: 1 }
).limit(10).forEach(printjson);

// Rango de fechas
print("\n--- Lecturas durante enero de 2024 ---");
db.iot_readings.find(
    {
        timestamp: {
            $gte: ISODate("2024-01-01T00:00:00Z"),
            $lt: ISODate("2024-02-01T00:00:00Z")
        }
    },
    { _id: 0, sensor_id: 1, timestamp: 1, mediciones: 1 }
).limit(10).forEach(printjson);

// Múltiples filtros: calidad del aire, zona y PM2.5 alto
print("\n--- PM2.5 superior a 35 en Z01 ---");
db.iot_readings.find(
    {
        tipo: "calidad_aire",
        "zona.id": "Z01",
        "mediciones.pm25": { $gt: 35 }
    },
    { _id: 0, sensor_id: 1, zona: 1, timestamp: 1, "mediciones.pm25": 1 }
).sort({ "mediciones.pm25": -1 }).limit(10).forEach(printjson);

// Ordenamiento: mayores niveles de ruido
print("\n--- Diez lecturas con mayor ruido ---");
db.iot_readings.find(
    { tipo: "ruido" },
    { _id: 0, sensor_id: 1, zona: 1, timestamp: 1, "mediciones.nivel_db": 1 }
).sort({ "mediciones.nivel_db": -1 }).limit(10).forEach(printjson);


print("\n========== 3. MÁXIMOS Y MÍNIMOS ==========");

// Máximo de PM2.5
print("\n--- Máximo PM2.5 ---");
printjson(
    db.iot_readings.find(
        { tipo: "calidad_aire", "mediciones.pm25": { $ne: null } }
    ).sort({ "mediciones.pm25": -1 }).limit(1).toArray()
);

// Mínimo de PM2.5
print("\n--- Mínimo PM2.5 ---");
printjson(
    db.iot_readings.find(
        { tipo: "calidad_aire", "mediciones.pm25": { $ne: null } }
    ).sort({ "mediciones.pm25": 1 }).limit(1).toArray()
);

// Estadísticas de ruido
print("\n--- Máximo y mínimo de ruido ---");
printjson(
    db.iot_readings.aggregate([
        { $match: { tipo: "ruido", "mediciones.nivel_db": { $ne: null } } },
        {
            $group: {
                _id: null,
                maximo: { $max: "$mediciones.nivel_db" },
                minimo: { $min: "$mediciones.nivel_db" },
                promedio: { $avg: "$mediciones.nivel_db" }
            }
        }
    ]).toArray()
);


print("\n========== 4. AGREGACIONES ==========");

// Promedio de mediciones por zona y variable
print("\n--- Promedios por zona y variable ---");
db.iot_readings.aggregate([
    { $project: {
        zona: "$zona.nombre",
        tipo: 1,
        mediciones: { $objectToArray: "$mediciones" }
    }},
    { $unwind: "$mediciones" },
    { $match: { "mediciones.v": { $ne: null } } },
    {
        $group: {
            _id: {
                zona: "$zona",
                tipo: "$tipo",
                variable: "$mediciones.k"
            },
            promedio: { $avg: "$mediciones.v" },
            cantidad: { $sum: 1 }
        }
    },
    { $sort: { "_id.zona": 1, "_id.tipo": 1, "_id.variable": 1 } }
]).forEach(printjson);

// Promedio de PM2.5 por zona
print("\n--- Promedio de PM2.5 por zona ---");
db.iot_readings.aggregate([
    { $match: { tipo: "calidad_aire", "mediciones.pm25": { $ne: null } } },
    {
        $group: {
            _id: "$zona.nombre",
            promedio_pm25: { $avg: "$mediciones.pm25" },
            maximo_pm25: { $max: "$mediciones.pm25" },
            lecturas: { $sum: 1 }
        }
    },
    { $sort: { promedio_pm25: -1 } }
]).forEach(printjson);

// Cantidad de lecturas por tipo de sensor
print("\n--- Lecturas por tipo ---");
db.iot_readings.aggregate([
    { $group: { _id: "$tipo", total: { $sum: 1 } } },
    { $sort: { total: -1 } }
]).forEach(printjson);

// Sensores más activos
print("\n--- Sensores con más lecturas ---");
db.iot_readings.aggregate([
    { $group: { _id: "$sensor_id", lecturas: { $sum: 1 } } },
    { $sort: { lecturas: -1 } },
    { $limit: 10 }
]).forEach(printjson);


print("\n========== 5. ALERTAS DERIVADAS DE MEDICIONES ==========");

// Se aplican umbrales definidos en la configuración del proyecto.
// Son alertas calculadas; no son documentos persistidos en otra colección.

print("\n--- Mediciones críticas ---");
db.iot_readings.aggregate([
    {
        $match: {
            $or: [
                { tipo: "calidad_aire", "mediciones.pm25": { $gt: 35 } },
                { tipo: "calidad_aire", "mediciones.pm10": { $gt: 50 } },
                { tipo: "calidad_aire", "mediciones.no2": { $gt: 200 } },
                { tipo: "calidad_aire", "mediciones.co": { $gt: 10 } },
                { tipo: "calidad_aire", "mediciones.co2": { $gt: 1000 } },
                { tipo: "ruido", "mediciones.nivel_db": { $gt: 70 } },
                { tipo: "trafico", "mediciones.velocidad_promedio": { $lt: 15 } },
                { tipo: "estacionamiento", "mediciones.ocupacion_pct": { $gt: 95 } }
            ]
        }
    },
    {
        $project: {
            _id: 0,
            sensor_id: 1,
            tipo: 1,
            zona: "$zona.nombre",
            timestamp: 1,
            mediciones: 1
        }
    },
    { $sort: { timestamp: -1 } },
    { $limit: 20 }
]).forEach(printjson);

// Cantidad de alertas derivadas por zona
print("\n--- Alertas derivadas por zona ---");
db.iot_readings.aggregate([
    {
        $match: {
            $or: [
                { tipo: "calidad_aire", "mediciones.pm25": { $gt: 35 } },
                { tipo: "calidad_aire", "mediciones.pm10": { $gt: 50 } },
                { tipo: "calidad_aire", "mediciones.no2": { $gt: 200 } },
                { tipo: "calidad_aire", "mediciones.co": { $gt: 10 } },
                { tipo: "calidad_aire", "mediciones.co2": { $gt: 1000 } },
                { tipo: "ruido", "mediciones.nivel_db": { $gt: 70 } },
                { tipo: "trafico", "mediciones.velocidad_promedio": { $lt: 15 } },
                { tipo: "estacionamiento", "mediciones.ocupacion_pct": { $gt: 95 } }
            ]
        }
    },
    {
        $group: {
            _id: "$zona.nombre",
            alertas_derivadas: { $sum: 1 }
        }
    },
    { $sort: { alertas_derivadas: -1 } }
]).forEach(printjson);


print("\n========== 6. ÍNDICES Y CONSULTA GEOESPACIAL ==========");

// Índice para acelerar consultas por tipo, zona y fecha
db.iot_readings.createIndex(
    { tipo: 1, "zona.id": 1, timestamp: 1 },
    { name: "idx_tipo_zona_timestamp" }
);

// Índice geoespacial para ubicaciones GeoJSON
db.iot_readings.createIndex(
    { ubicacion: "2dsphere" },
    { name: "idx_ubicacion_2dsphere" }
);

// Consultar lecturas con ubicación registrada
print("\n--- Lecturas con coordenadas ---");
db.iot_readings.find(
    { ubicacion: { $exists: true } },
    { _id: 0, sensor_id: 1, zona: 1, ubicacion: 1 }
).limit(10).forEach(printjson);


print("\n========== 7. COLECCIÓN ORIGINAL DE CONTAMINACIÓN ==========");

// Máximos y promedios de la colección pollution_measurements
print("\n--- Estadísticas de contaminación original ---");
db.pollution_measurements.aggregate([
    {
        $group: {
            _id: null,
            registros: { $sum: 1 },
            promedio_ozono: { $avg: "$ozone" },
            promedio_particulas: { $avg: "$particullate_matter" },
            maximo_ozono: { $max: "$ozone" },
            maximo_particulas: { $max: "$particullate_matter" }
        }
    }
]).forEach(printjson);

print("\n========== FIN DE LAS CONSULTAS ==========");
