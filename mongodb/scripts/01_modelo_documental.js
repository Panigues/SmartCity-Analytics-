// =====================================================================
// SMARTCITY ANALYTICS - MODELO DOCUMENTAL EN MONGODB
//
// El enunciado pide que MongoDB almacene:
//   - Lecturas de sensores ............ iot_readings  (ya importada)
//   - Alertas automáticas ............. alertas       (este script, con $merge)
//   - Incidentes urbanos .............. incidentes    (este script)
//   - Información dinámica no estructurada  -> texto libre, adjuntos,
//     seguimiento y campos que cambian según la categoría del incidente
//   - Eventos detectados .............. eventos       (02_eventos_desde_spark.py)
//
// Ejecutar (después de importar iot_readings):
//   mongosh smartcity mongodb/scripts/01_modelo_documental.js
//   (con el contenedor: docker exec -i smartcity-mongo mongosh smartcity < mongodb/scripts/01_modelo_documental.js)
//
// Es re-ejecutable: reconstruye alertas e incidentes en cada corrida.
// =====================================================================

db = db.getSiblingDB("smartcity");

if (db.iot_readings.countDocuments({}, { limit: 1 }) === 0) {
    throw new Error("La colección iot_readings está vacía. Importe primero las lecturas.");
}

// ---------------------------------------------------------------------
// 1. COLECCIONES CON VALIDACIÓN FLEXIBLE ($jsonSchema)
// Solo se exigen los campos mínimos; el resto del documento es libre.
// Así se demuestra la flexibilidad del modelo: cada incidente puede
// traer campos distintos sin romper la validación.
// ---------------------------------------------------------------------
print("\n=== 1. Colecciones y validaciones ===");

function crearColeccion(nombre, esquema) {
    if (db.getCollectionNames().includes(nombre)) {
        db.runCommand({ collMod: nombre, validator: { $jsonSchema: esquema }, validationLevel: "moderate" });
    } else {
        db.createCollection(nombre, { validator: { $jsonSchema: esquema }, validationLevel: "moderate" });
    }
    print("  colección " + nombre + " lista");
}

crearColeccion("alertas", {
    bsonType: "object",
    required: ["sensor_id", "zona", "timestamp", "variable", "valor", "limite", "severidad"],
    properties: {
        valor: { bsonType: ["double", "int", "long", "decimal"] },
        severidad: { enum: ["media", "alta", "critica"] }
    }
});

crearColeccion("incidentes", {
    bsonType: "object",
    required: ["codigo", "categoria", "zona", "fecha_reporte", "estado", "descripcion"],
    properties: {
        categoria: { bsonType: "string" },
        estado: { enum: ["reportado", "en_atencion", "resuelto", "cerrado"] },
        descripcion: { bsonType: "string", minLength: 10 }
    }
});

crearColeccion("eventos", {
    bsonType: "object",
    required: ["tipo_evento", "origen", "sensor_id", "zona", "timestamp"],
    properties: { origen: { bsonType: "string" } }
});

// ---------------------------------------------------------------------
// 2. ALERTAS AUTOMÁTICAS
// Se generan con el Aggregation Framework a partir de las lecturas:
// cada medición que supera su límite produce un documento de alerta.
// $merge las guarda en la colección (y permite re-ejecutar sin duplicar).
// ---------------------------------------------------------------------
print("\n=== 2. Alertas automáticas (Aggregation Framework + $merge) ===");

const LIMITES = { pm25: 35, pm10: 50, no2: 200, co: 10, co2: 1000, nivel_db: 70, ocupacion_pct: 95 };

db.alertas.deleteMany({});
const inicioAlertas = new Date();

db.iot_readings.aggregate([
    // Filtro previo: solo documentos con alguna medición fuera de límite
    { $match: { $or: [
        ...Object.entries(LIMITES).map(([v, l]) => ({ ["mediciones." + v]: { $gt: l } })),
        { "mediciones.velocidad_promedio": { $lt: 15 } }
    ] } },
    { $project: { sensor_id: 1, tipo: 1, categoria: 1, zona: 1, ubicacion: 1, timestamp: 1,
                  m: { $objectToArray: "$mediciones" } } },
    { $unwind: "$m" },
    { $addFields: {
        variable: "$m.k",
        valor: "$m.v",
        limite: { $switch: {
            branches: [
                ...Object.entries(LIMITES).map(([v, l]) => ({ case: { $eq: ["$m.k", v] }, then: l })),
                { case: { $eq: ["$m.k", "velocidad_promedio"] }, then: 15 }
            ],
            default: null } }
    } },
    { $match: { limite: { $ne: null } } },
    { $match: { $expr: { $cond: [
        { $eq: ["$variable", "velocidad_promedio"] },
        { $lt: ["$valor", "$limite"] },          // velocidad: alerta si está por DEBAJO
        { $gt: ["$valor", "$limite"] } ] } } },  // resto: alerta si está por ENCIMA
    { $addFields: {
        exceso_pct: { $round: [{ $multiply: [{ $divide: [{ $abs: { $subtract: ["$valor", "$limite"] } }, "$limite"] }, 100] }, 1] }
    } },
    { $project: {
        _id: { $concat: ["$sensor_id", "|", { $toString: "$timestamp" }, "|", "$variable"] },
        sensor_id: 1, tipo: 1, categoria: 1, zona: 1, ubicacion: 1, timestamp: 1,
        variable: 1, valor: 1, limite: 1, exceso_pct: 1,
        severidad: { $switch: { branches: [
            { case: { $gte: ["$exceso_pct", 100] }, then: "critica" },
            { case: { $gte: ["$exceso_pct", 30] }, then: "alta" } ], default: "media" } },
        regla: { $concat: ["$variable",
                           { $cond: [{ $eq: ["$variable", "velocidad_promedio"] }, " < ", " > "] },
                           { $toString: "$limite" }] },
        estado: "activa",
        generada_en: "$$NOW"
    } },
    { $merge: { into: "alertas", on: "_id", whenMatched: "replace", whenNotMatched: "insert" } }
], { allowDiskUse: true });

print("  alertas generadas: " + db.alertas.countDocuments() +
      " (" + ((new Date() - inicioAlertas) / 1000).toFixed(1) + " s)");
db.alertas.aggregate([
    { $group: { _id: "$severidad", total: { $sum: 1 } } }, { $sort: { total: -1 } }
]).forEach(r => print("    " + r._id + ": " + r.total));

// ---------------------------------------------------------------------
// 3. INCIDENTES URBANOS (información dinámica no estructurada)
// Cada categoría trae campos distintos: un accidente registra vehículos
// y heridos, una inundación el nivel del agua, un sensor dañado su id.
// Algunos tienen fotos, testigos o un historial de seguimiento; otros no.
// En PostgreSQL esto exigiría una tabla por categoría o muchas columnas
// vacías; en MongoDB cada documento trae solo lo que necesita.
// ---------------------------------------------------------------------
print("\n=== 3. Incidentes urbanos ===");

// Generador pseudoaleatorio con semilla fija (resultados reproducibles)
let semilla = 42;
function azar() {                       // mulberry32
    semilla |= 0; semilla = (semilla + 0x6D2B79F5) | 0;
    let t = Math.imul(semilla ^ (semilla >>> 15), 1 | semilla);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}
function elegir(lista) { return lista[Math.floor(azar() * lista.length)]; }
function entero(min, max) { return min + Math.floor(azar() * (max - min + 1)); }

const rango = db.iot_readings.aggregate([
    { $group: { _id: null, min: { $min: "$timestamp" }, max: { $max: "$timestamp" } } }
]).toArray()[0];

// Un sensor de referencia por zona y tipo, con su ubicación real
const sensores = db.iot_readings.aggregate([
    { $group: { _id: "$sensor_id", tipo: { $first: "$tipo" }, zona: { $first: "$zona" },
                ubicacion: { $first: "$ubicacion" } } }
]).toArray();

const CALLES = ["Randersvej", "Viborgvej", "Silkeborgvej", "Søndergade", "Grenåvej", "Ringgaden",
                "Skanderborgvej", "Nørrebrogade", "Frederiksgade", "Marselis Boulevard"];
const REPORTANTES = ["ciudadano", "app_municipal", "funcionario", "sensor_automatico", "linea_911"];
const CATEGORIAS = {
    accidente_transito: { tipos: ["trafico"], prioridad: ["alta", "critica"] },
    congestion_severa: { tipos: ["trafico"], prioridad: ["media", "alta"] },
    inundacion: { tipos: ["clima"], prioridad: ["alta", "critica"] },
    falla_electrica: { tipos: ["energia"], prioridad: ["alta", "critica"] },
    contaminacion_aire: { tipos: ["calidad_aire"], prioridad: ["media", "alta"] },
    ruido_excesivo: { tipos: ["ruido"], prioridad: ["baja", "media"] },
    sensor_danado: { tipos: ["energia", "calidad_aire", "clima", "ruido", "trafico", "estacionamiento"], prioridad: ["media"] },
    estacionamiento_saturado: { tipos: ["estacionamiento"], prioridad: ["baja", "media"] }
};

function descripcion(cat, calle, extra) {
    const textos = {
        accidente_transito: `Colisión entre ${extra.vehiculos} vehículos en ${calle}. ` +
            (extra.heridos ? `Se reportan ${extra.heridos} personas heridas; se solicitó ambulancia.` : "Sin heridos, solo daños materiales.") +
            " Un carril permanece cerrado.",
        congestion_severa: `Tráfico detenido en ${calle} por más de ${extra.minutos} minutos. Los conductores reportan una fila de unos ${extra.km} km.`,
        inundacion: `Acumulación de agua de aproximadamente ${extra.nivel} cm en ${calle} tras lluvias intensas. Alcantarillas obstruidas.`,
        falla_electrica: `Corte de energía que afecta a cerca de ${extra.hogares} hogares en los alrededores de ${calle}. Se observa daño en un transformador.`,
        contaminacion_aire: `Vecinos de ${calle} reportan olor fuerte y humo visible. Las lecturas de material particulado están elevadas.`,
        ruido_excesivo: `Denuncia por ruido constante en ${calle}, posiblemente por obras nocturnas o un evento al aire libre.`,
        sensor_danado: `El sensor ${extra.sensor} dejó de reportar o envía valores incoherentes. Requiere revisión técnica en sitio.`,
        estacionamiento_saturado: `El estacionamiento cercano a ${calle} está lleno; vehículos esperan en la vía y obstruyen el paso.`
    };
    return textos[cat];
}

db.incidentes.deleteMany({});
const incidentes = [];
const duracion = rango.max - rango.min;
for (let i = 1; i <= 400; i++) {
    const cat = elegir(Object.keys(CATEGORIAS));
    const def = CATEGORIAS[cat];
    const candidatos = sensores.filter(s => def.tipos.includes(s.tipo));
    const ref = elegir(candidatos);
    const fecha = new Date(rango.min.getTime() + azar() * duracion);
    const calle = elegir(CALLES);
    const extra = { vehiculos: entero(2, 4), heridos: azar() < 0.4 ? entero(1, 3) : 0, minutos: entero(30, 120),
                    km: entero(1, 5), nivel: entero(10, 60), hogares: entero(50, 900), sensor: ref._id };

    const doc = {
        codigo: "INC-" + String(i).padStart(5, "0"),
        categoria: cat,
        prioridad: elegir(def.prioridad),
        zona: ref.zona,
        ubicacion: ref.ubicacion,
        direccion: calle + " " + entero(1, 220),
        fecha_reporte: fecha,
        reportado_por: { tipo: elegir(REPORTANTES) },
        descripcion: descripcion(cat, calle, extra),
        estado: elegir(["reportado", "en_atencion", "resuelto", "resuelto", "cerrado"]),
        etiquetas: [cat.split("_")[0], ref.zona.nombre.toLowerCase()]
    };

    // Campos que solo existen según la categoría
    if (cat === "accidente_transito") {
        doc.vehiculos_involucrados = Array.from({ length: extra.vehiculos }, () => {
            const v = { tipo: elegir(["auto", "auto", "moto", "bus", "camion", "bicicleta"]) };
            if (v.tipo !== "bicicleta") v.placa = "AA" + entero(10000, 99999);   // las bicicletas no tienen placa
            return v;
        });
        doc.heridos = extra.heridos;
        if (extra.heridos) doc.servicios_emergencia = ["ambulancia", "policia"];
    }
    if (cat === "inundacion") { doc.nivel_agua_cm = extra.nivel; doc.vias_cerradas = entero(1, 3); }
    if (cat === "falla_electrica") { doc.hogares_afectados = extra.hogares; doc.tiempo_sin_servicio_min = entero(20, 300); }
    if (cat === "congestion_severa") { doc.longitud_fila_km = extra.km; }
    if (cat === "sensor_danado") { doc.sensor_id = ref._id; doc.diagnostico = elegir(["sin señal", "batería baja", "valores fuera de rango", "daño físico"]); }
    if (["contaminacion_aire", "ruido_excesivo", "estacionamiento_saturado"].includes(cat) || azar() < 0.3) {
        doc.sensor_relacionado = ref._id;
    }

    // Información opcional y variable
    if (azar() < 0.45) {
        doc.adjuntos = Array.from({ length: entero(1, 3) }, (_, k) =>
            ({ tipo: "foto", archivo: `${doc.codigo}_${k + 1}.jpg`, tamano_kb: entero(300, 4000) }));
    }
    if (azar() < 0.25) doc.testigos = entero(1, 6);
    if (doc.reportado_por.tipo === "ciudadano") {
        doc.reportado_por.canal = elegir(["telefono", "redes_sociales", "presencial"]);
        if (azar() < 0.5) doc.reportado_por.comentario = "Pasa seguido en este punto, por favor atender pronto.";
    }
    if (doc.estado !== "reportado") {
        doc.seguimiento = [{ fecha: new Date(fecha.getTime() + entero(10, 90) * 60000), estado: "en_atencion",
                             nota: "Cuadrilla asignada al caso." }];
        if (["resuelto", "cerrado"].includes(doc.estado)) {
            doc.seguimiento.push({ fecha: new Date(fecha.getTime() + entero(2, 48) * 3600000), estado: doc.estado,
                                   nota: "Situación atendida y verificada en sitio." });
        }
    }
    incidentes.push(doc);
}
db.incidentes.insertMany(incidentes);
print("  incidentes insertados: " + db.incidentes.countDocuments());

// ---------------------------------------------------------------------
// 4. ÍNDICES
// ---------------------------------------------------------------------
print("\n=== 4. Índices ===");
db.iot_readings.createIndex({ sensor_id: 1, timestamp: 1 }, { name: "idx_sensor_timestamp" });
db.iot_readings.createIndex({ tipo: 1, "zona.id": 1, timestamp: 1 }, { name: "idx_tipo_zona_timestamp" });
db.iot_readings.createIndex({ ubicacion: "2dsphere" }, { name: "idx_ubicacion_2dsphere" });
db.alertas.createIndex({ "zona.id": 1, timestamp: 1 }, { name: "idx_alerta_zona_fecha" });
db.alertas.createIndex({ variable: 1, severidad: 1 }, { name: "idx_alerta_variable_severidad" });
db.incidentes.createIndex({ categoria: 1, estado: 1 }, { name: "idx_incidente_categoria_estado" });
db.incidentes.createIndex({ ubicacion: "2dsphere" }, { name: "idx_incidente_2dsphere" });
db.incidentes.createIndex({ descripcion: "text" }, { name: "idx_incidente_texto", default_language: "spanish" });
db.eventos.createIndex({ "zona.id": 1, timestamp: 1 }, { name: "idx_evento_zona_fecha" });
["iot_readings", "alertas", "incidentes", "eventos"].forEach(c =>
    print("  " + c + ": " + db[c].getIndexes().map(i => i.name).join(", ")));

print("\n=== Modelo documental listo ===");
["iot_readings", "alertas", "incidentes", "eventos"].forEach(c =>
    print("  " + c.padEnd(14) + db[c].countDocuments()));
print("Siguiente paso: python mongodb/scripts/02_eventos_desde_spark.py");
