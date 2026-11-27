// ============================================================
// SMARTCITY ANALYTICS - CONSULTAS MONGODB
// Base de datos: smartcity
// Colecciones: iot_readings, alertas, incidentes, eventos, pollution_measurements
//
// Requisito previo: 01_modelo_documental.js y 02_eventos_desde_spark.py
// Ejecutar:  mongosh smartcity mongodb/scripts/consultas_mongodb.js
// ============================================================

db = db.getSiblingDB("smartcity");

function titulo(t) { print("\n" + "=".repeat(64) + "\n" + t + "\n" + "=".repeat(64)); }
function sub(t) { print("\n--- " + t + " ---"); }
function mostrar(cursor) { cursor.forEach(d => printjson(d)); }


titulo("1. OPERACIONES CRUD (sobre documentos de prueba Z99-TEST)");
// Se ejecutan de verdad, pero sobre un sensor ficticio de la zona Z99,
// y al final se eliminan: los datos reales no se modifican.

db.iot_readings.deleteMany({ sensor_id: /^Z99-TEST-/ });   // limpieza de corridas anteriores

sub("insertOne");
let r = db.iot_readings.insertOne({
    sensor_id: "Z99-TEST-001", tipo: "ruido", categoria: "ambiente",
    zona: { id: "Z99", nombre: "Prueba" },
    timestamp: new Date("2025-06-01T10:00:00Z"),
    mediciones: { nivel_db: 55 }
});
print("insertado:", r.insertedId);

sub("insertMany (documentos con estructuras distintas)");
r = db.iot_readings.insertMany([
    { sensor_id: "Z99-TEST-002", tipo: "ruido", categoria: "ambiente", zona: { id: "Z99", nombre: "Prueba" },
      timestamp: new Date("2025-06-01T10:00:00Z"), mediciones: { nivel_db: 72 } },
    { sensor_id: "Z99-TEST-003", tipo: "calidad_aire", categoria: "ambiente", zona: { id: "Z99", nombre: "Prueba" },
      timestamp: new Date("2025-06-01T10:00:00Z"), mediciones: { pm25: 41.2, pm10: 60.5, no2: 25.1 } },
    { sensor_id: "Z99-TEST-004", tipo: "trafico", categoria: "movilidad", zona: { id: "Z99", nombre: "Prueba" },
      timestamp: new Date("2025-06-01T10:00:00Z"), mediciones: { vehiculos: 120, velocidad_promedio: 12.5 },
      nota_tecnico: "sensor recién calibrado" }         // campo que los demás documentos no tienen
]);
print("insertados:", Object.keys(r.insertedIds).length);

sub("find");
mostrar(db.iot_readings.find({ sensor_id: /^Z99-TEST-/ }, { _id: 0, sensor_id: 1, tipo: 1, mediciones: 1 }));

sub("findOne");
printjson(db.iot_readings.findOne({ sensor_id: "Z99-TEST-003" }, { _id: 0, sensor_id: 1, mediciones: 1 }));

sub("updateOne: corregir una medición");
r = db.iot_readings.updateOne({ sensor_id: "Z99-TEST-001" }, { $set: { "mediciones.nivel_db": 58 } });
print("modificados:", r.modifiedCount);

sub("updateMany: marcar todos los documentos de prueba como revisados");
r = db.iot_readings.updateMany({ sensor_id: /^Z99-TEST-/ },
                               { $set: { revisado: true }, $currentDate: { revisado_en: true } });
print("modificados:", r.modifiedCount);

sub("deleteOne");
r = db.iot_readings.deleteOne({ sensor_id: "Z99-TEST-001" });
print("eliminados:", r.deletedCount);

sub("limpieza final (deleteMany)");
r = db.iot_readings.deleteMany({ sensor_id: /^Z99-TEST-/ });
print("eliminados:", r.deletedCount, "· quedan de prueba:", db.iot_readings.countDocuments({ sensor_id: /^Z99-TEST-/ }));


titulo("2. CONSULTAS OBLIGATORIAS");

sub("CONSULTA 1. Lecturas de un sensor específico (filtro + proyección + ordenamiento)");
mostrar(db.iot_readings.find(
    { sensor_id: "Z01-AIR-001" },
    { _id: 0, sensor_id: 1, timestamp: 1, mediciones: 1 }
).sort({ timestamp: 1 }).limit(5));

sub("CONSULTA 2. Lecturas en un rango de fechas (operadores $gte / $lt)");
mostrar(db.iot_readings.find(
    { timestamp: { $gte: ISODate("2024-01-01T00:00:00Z"), $lt: ISODate("2024-01-01T03:00:00Z") },
      "zona.id": "Z01", tipo: "calidad_aire" },
    { _id: 0, sensor_id: 1, timestamp: 1, "mediciones.pm25": 1, "mediciones.pm10": 1 }
).sort({ timestamp: 1, sensor_id: 1 }).limit(10));

sub("CONSULTA 3. Valores máximos y mínimos registrados por tipo y variable");
mostrar(db.iot_readings.aggregate([
    { $project: { tipo: 1, m: { $objectToArray: "$mediciones" } } },
    { $unwind: "$m" },
    { $group: { _id: { tipo: "$tipo", variable: "$m.k" },
                minimo: { $min: "$m.v" }, maximo: { $max: "$m.v" } } },
    { $project: { _id: 0, tipo: "$_id.tipo", variable: "$_id.variable",
                  minimo: { $round: ["$minimo", 2] }, maximo: { $round: ["$maximo", 2] } } },
    { $sort: { tipo: 1, variable: 1 } }
], { allowDiskUse: true }));

sub("CONSULTA 4. Eventos críticos detectados");
print("Alertas automáticas de severidad crítica (colección alertas):");
mostrar(db.alertas.find(
    { severidad: "critica" },
    { _id: 0, sensor_id: 1, "zona.nombre": 1, timestamp: 1, variable: 1, valor: 1, limite: 1, exceso_pct: 1 }
).sort({ exceso_pct: -1 }).limit(5));
print("Anomalías críticas detectadas por Spark (colección eventos):");
mostrar(db.eventos.find(
    { severidad: "critica" },
    { _id: 0, sensor_id: 1, "zona.nombre": 1, timestamp: 1, variable: 1, valor: 1, esperado: 1, puntaje: 1 }
).sort({ puntaje: -1 }).limit(5));

sub("CONSULTA 5. Alertas emitidas por zona geográfica");
mostrar(db.alertas.aggregate([
    { $group: { _id: "$zona.nombre", alertas: { $sum: 1 },
                criticas: { $sum: { $cond: [{ $eq: ["$severidad", "critica"] }, 1, 0] } },
                variables: { $addToSet: "$variable" } } },
    { $sort: { alertas: -1 } }
]));
print("Alertas a menos de 1,5 km del centro de Aarhus (consulta geoespacial $geoWithin):");
printjson(db.alertas.countDocuments({
    ubicacion: { $geoWithin: { $centerSphere: [[10.2107, 56.1572], 1.5 / 6378.1] } }
}));

sub("CONSULTA 6. Promedio de mediciones por zona");
mostrar(db.iot_readings.aggregate([
    { $project: { zona: "$zona.nombre", m: { $objectToArray: "$mediciones" } } },
    { $unwind: "$m" },
    { $match: { "m.k": { $in: ["temperatura", "pm25", "no2", "nivel_db", "consumo_kwh", "vehiculos"] } } },
    { $group: { _id: { zona: "$zona", variable: "$m.k" }, promedio: { $avg: "$m.v" } } },
    { $group: { _id: "$_id.zona",
                promedios: { $push: { k: "$_id.variable", v: { $round: ["$promedio", 2] } } } } },
    { $project: { _id: 0, zona: "$_id", promedios: { $arrayToObject: "$promedios" } } },
    { $sort: { zona: 1 } }
], { allowDiskUse: true }));

sub("CONSULTA 7. Promedio por tipo de sensor");
mostrar(db.iot_readings.aggregate([
    { $project: { tipo: 1, sensor_id: 1, m: { $objectToArray: "$mediciones" } } },
    { $unwind: "$m" },
    { $group: { _id: { tipo: "$tipo", variable: "$m.k" }, promedio: { $avg: "$m.v" },
                lecturas: { $sum: 1 }, sensores: { $addToSet: "$sensor_id" } } },
    { $project: { _id: 0, tipo: "$_id.tipo", variable: "$_id.variable",
                  promedio: { $round: ["$promedio", 2] }, lecturas: 1, sensores: { $size: "$sensores" } } },
    { $sort: { tipo: 1, variable: 1 } }
], { allowDiskUse: true }));

sub("CONSULTA 8. Incidentes registrados por categoría");
mostrar(db.incidentes.aggregate([
    { $group: { _id: "$categoria", incidentes: { $sum: 1 },
                abiertos: { $sum: { $cond: [{ $in: ["$estado", ["reportado", "en_atencion"]] }, 1, 0] } },
                zonas: { $addToSet: "$zona.nombre" } } },
    { $project: { _id: 0, categoria: "$_id", incidentes: 1, abiertos: 1, zonas: 1 } },
    { $sort: { incidentes: -1 } }
]));

sub("CONSULTA 9. Agregación: evolución diaria de PM2.5 y NO2 por zona (primera semana de enero 2024)");
mostrar(db.iot_readings.aggregate([
    { $match: { tipo: "calidad_aire",
                timestamp: { $gte: ISODate("2024-01-01T00:00:00Z"), $lt: ISODate("2024-01-08T00:00:00Z") } } },
    { $group: { _id: { dia: { $dateToString: { format: "%Y-%m-%d", date: "$timestamp" } }, zona: "$zona.nombre" },
                pm25: { $avg: "$mediciones.pm25" }, no2: { $avg: "$mediciones.no2" }, lecturas: { $sum: 1 } } },
    { $project: { _id: 0, dia: "$_id.dia", zona: "$_id.zona", lecturas: 1,
                  pm25: { $round: ["$pm25", 2] }, no2: { $round: ["$no2", 2] } } },
    { $sort: { dia: 1, zona: 1 } }
]));

sub("CONSULTA 10. Filtros múltiples: aire en Centro o Sur, PM2.5 > 25 y NO2 > 30, en 2024");
mostrar(db.iot_readings.find(
    { tipo: "calidad_aire",
      "zona.id": { $in: ["Z01", "Z03"] },
      "mediciones.pm25": { $gt: 25 },
      "mediciones.no2": { $gt: 30 },
      timestamp: { $gte: ISODate("2024-01-01T00:00:00Z"), $lt: ISODate("2025-01-01T00:00:00Z") } },
    { _id: 0, sensor_id: 1, "zona.nombre": 1, timestamp: 1, "mediciones.pm25": 1, "mediciones.no2": 1 }
).sort({ "mediciones.pm25": -1 }).limit(10));


titulo("3. PREGUNTAS DEL ENUNCIADO");

sub("¿Cuál es la temperatura promedio por zona?");
mostrar(db.iot_readings.aggregate([
    { $match: { tipo: "clima" } },
    { $group: { _id: "$zona.nombre", temperatura: { $avg: "$mediciones.temperatura" },
                minima: { $min: "$mediciones.temperatura" }, maxima: { $max: "$mediciones.temperatura" } } },
    { $project: { _id: 0, zona: "$_id", temperatura: { $round: ["$temperatura", 2] },
                  minima: { $round: ["$minima", 1] }, maxima: { $round: ["$maxima", 1] } } },
    { $sort: { temperatura: -1 } }
]));

sub("¿Qué sensores registraron valores superiores al límite establecido?");
mostrar(db.alertas.aggregate([
    { $group: { _id: "$sensor_id", alertas: { $sum: 1 }, variables: { $addToSet: "$variable" },
                peor_exceso_pct: { $max: "$exceso_pct" } } },
    { $sort: { alertas: -1 } }, { $limit: 10 }
]));

sub("¿Cuántas mediciones produjo cada sensor? (los 10 con más)");
mostrar(db.iot_readings.aggregate([
    { $group: { _id: "$sensor_id", lecturas: { $sum: 1 },
                mediciones: { $sum: { $size: { $objectToArray: "$mediciones" } } } } },
    { $sort: { mediciones: -1, _id: 1 } }, { $limit: 10 }
]));

sub("¿Qué zonas presentan mayor concentración de contaminación?");
mostrar(db.iot_readings.aggregate([
    { $match: { tipo: "calidad_aire" } },
    { $group: { _id: "$zona.nombre", pm25: { $avg: "$mediciones.pm25" }, pm10: { $avg: "$mediciones.pm10" },
                no2: { $avg: "$mediciones.no2" } } },
    { $project: { _id: 0, zona: "$_id", pm25: { $round: ["$pm25", 2] }, pm10: { $round: ["$pm10", 2] },
                  no2: { $round: ["$no2", 2] } } },
    { $sort: { pm25: -1 } }
]));


titulo("4. FLEXIBILIDAD DEL MODELO DOCUMENTAL");

sub("Incidentes con campos que solo existen en algunas categorías ($exists)");
mostrar(db.incidentes.aggregate([
    { $group: { _id: "$categoria",
                con_vehiculos: { $sum: { $cond: [{ $gt: ["$vehiculos_involucrados", null] }, 1, 0] } },
                con_nivel_agua: { $sum: { $cond: [{ $gt: ["$nivel_agua_cm", null] }, 1, 0] } },
                con_fotos: { $sum: { $cond: [{ $gt: ["$adjuntos", null] }, 1, 0] } },
                con_seguimiento: { $sum: { $cond: [{ $gt: ["$seguimiento", null] }, 1, 0] } } } },
    { $sort: { _id: 1 } }
]));

sub("Búsqueda de texto libre en las descripciones ($text): 'heridos ambulancia'");
mostrar(db.incidentes.find(
    { $text: { $search: "heridos ambulancia" } },
    { _id: 0, codigo: 1, categoria: 1, descripcion: 1, puntaje: { $meta: "textScore" } }
).sort({ puntaje: { $meta: "textScore" } }).limit(3));

sub("Incidentes a menos de 1 km de un punto ($nearSphere con índice 2dsphere)");
mostrar(db.incidentes.find(
    { ubicacion: { $nearSphere: { $geometry: { type: "Point", coordinates: [10.2107, 56.1572] }, $maxDistance: 1000 } } },
    { _id: 0, codigo: 1, categoria: 1, direccion: 1, estado: 1 }
).limit(5));

sub("Relación entre colecciones ($lookup): incidentes de sensores dañados y sus alertas");
mostrar(db.incidentes.aggregate([
    { $match: { categoria: "sensor_danado" } },
    { $lookup: { from: "alertas", localField: "sensor_id", foreignField: "sensor_id", as: "alertas_del_sensor" } },
    { $project: { _id: 0, codigo: 1, sensor_id: 1, diagnostico: 1, estado: 1,
                  alertas_del_sensor: { $size: "$alertas_del_sensor" } } },
    { $sort: { alertas_del_sensor: -1 } }, { $limit: 5 }
]));


titulo("5. ÍNDICES");

db.iot_readings.createIndex({ sensor_id: 1, timestamp: 1 }, { name: "idx_sensor_timestamp" });
db.iot_readings.createIndex({ tipo: 1, "zona.id": 1, timestamp: 1 }, { name: "idx_tipo_zona_timestamp" });
db.iot_readings.createIndex({ ubicacion: "2dsphere" }, { name: "idx_ubicacion_2dsphere" });
print("Índices de iot_readings:", db.iot_readings.getIndexes().map(i => i.name).join(", "));

sub("Plan de ejecución de la consulta 1: usa el índice (IXSCAN) en lugar de recorrer la colección (COLLSCAN)");
const plan = db.iot_readings.find({ sensor_id: "Z01-AIR-001" }).sort({ timestamp: 1 }).explain("executionStats");
function etapas(p) { return p ? [p.stage].concat(etapas(p.inputStage)) : []; }
print("etapas:", etapas(plan.queryPlanner.winningPlan).join(" <- "));
print("documentos examinados:", plan.executionStats.totalDocsExamined,
      "· devueltos:", plan.executionStats.nReturned,
      "· tiempo:", plan.executionStats.executionTimeMillis, "ms");


if (db.getCollectionNames().includes("pollution_measurements")) {
    titulo("6. COLECCIÓN ORIGINAL DE CONTAMINACIÓN (CityPulse)");
    mostrar(db.pollution_measurements.aggregate([
        { $group: { _id: null, registros: { $sum: 1 },
                    promedio_ozono: { $avg: "$ozone" }, promedio_particulas: { $avg: "$particullate_matter" },
                    maximo_ozono: { $max: "$ozone" }, maximo_particulas: { $max: "$particullate_matter" } } }
    ]));
}

print("\n========== FIN DE LAS CONSULTAS ==========");
