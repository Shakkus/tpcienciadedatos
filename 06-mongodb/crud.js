// 06-mongodb/crud.js
// Comparación SQL vs documento (8 líneas):
// 1) En SQL, el catálogo comercial suele requerir 3 joins: producto, precio y stock.
// 2) En Mongo, el documento completo viaja en una sola entidad con precios y stock anidados.
// 3) El acceso a cada SKU es más directo para la app de ventas y recomendación.
// 4) La lista de tags permite filtros rápidos sin UNIR tablas adicionales.
// 5) Un documento anidado reduce latencia en reads frecuentes de catálogo.
// 6) El modelo relacional exige normalización y más joins en reporting.
// 7) Mongo prioriza lectura de un producto completo por clave o por segmento.
// 8) El objetivo es evitar joins costosos y mantener el catálogo comercial en un solo doc.

const fs = require("fs");
const { MongoClient } = require("mongodb");

const uri = "mongodb://127.0.0.1:27018";
const dbName = "nexo";
const collectionName = "productos";
const dataPath = "./data/productos.json";

async function main() {
  const client = new MongoClient(uri);
  await client.connect();

  const db = client.db(dbName);
  const productos = db.collection(collectionName);

  // 1) Reset + carga inicial
  const docs = JSON.parse(fs.readFileSync(dataPath, "utf8"));
  await productos.deleteMany({});
  await productos.insertMany(docs);

  // 2) Insert de un SKU discontinuado para deleteOne
  const skuDescontinuado = {
    sku: "SKU999",
    nombre: "Seguro Plus",
    categoria: "seguro",
    tags: ["anual", "familia"],
    precios: {
      "Buenos Aires": 15900,
      "Cordoba": 12800
    },
    stock: {
      "Buenos Aires": 35,
      "Cordoba": 18
    },
    activo: false
  };
  await productos.insertOne(skuDescontinuado);

  // 3) Find con filtro
  const productosFiltrados = await productos
    .find({ categoria: "pase", activo: true })
    .project({
      _id: 0,
      sku: 1,
      nombre: 1,
      categoria: 1,
      "precios.Buenos Aires": 1,
      "stock.Buenos Aires": 1
    })
    .limit(5)
    .toArray();

  console.log("Productos filtrados:", JSON.stringify(productosFiltrados, null, 2));

  // 4) UpdateOne de precio
  const updateResult = await productos.updateOne(
    { sku: "SKU001" },
    {
      $set: {
        "precios.Buenos Aires": 6490,
        "stock.Buenos Aires": 180
      }
    }
  );

  console.log("UpdateOne:", JSON.stringify(updateResult, null, 2));

  // 5) Explain antes del índice
  const beforeIndex = await productos
    .find({ categoria: "pase", tags: "urbano" })
    .explain("executionStats");

  // 6) Crear índice
  await productos.createIndex({ categoria: 1, tags: 1 });

  // 7) Explain después del índice
  const afterIndex = await productos
    .find({ categoria: "pase", tags: "urbano" })
    .explain("executionStats");

  // 8) DeleteOne del SKU discontinuado
  const deleteResult = await productos.deleteOne({ sku: "SKU999" });

  console.log("DeleteOne:", JSON.stringify(deleteResult, null, 2));

  const summary = {
    totalDocsInserted: docs.length + 1,
    totalDocsAfterDelete: await productos.countDocuments(),
    filteredCount: productosFiltrados.length,
    updateMatchedCount: updateResult.matchedCount,
    updateModifiedCount: updateResult.modifiedCount,
    deleteDeletedCount: deleteResult.deletedCount,
    beforeIndex: {
      executionTimeMillis: beforeIndex.executionStats.executionTimeMillis,
      totalDocsExamined: beforeIndex.executionStats.totalDocsExamined,
      totalKeysExamined: beforeIndex.executionStats.totalKeysExamined,
      nReturned: beforeIndex.executionStats.nReturned
    },
    afterIndex: {
      executionTimeMillis: afterIndex.executionStats.executionTimeMillis,
      totalDocsExamined: afterIndex.executionStats.totalDocsExamined,
      totalKeysExamined: afterIndex.executionStats.totalKeysExamined,
      nReturned: afterIndex.executionStats.nReturned
    },
    indexCreated: { categoria: 1, tags: 1 }
  };

  console.log("Resumen:", JSON.stringify(summary, null, 2));

  // 9) Export para evidencia
  const outDir = "./evidence/mongodb";
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(
    `${outDir}/index_explain.txt`,
    JSON.stringify(
      {
        query: 'db.productos.find({ categoria: "pase", tags: "urbano" })',
        beforeIndex: summary.beforeIndex,
        afterIndex: summary.afterIndex,
        improvementMs:
          summary.beforeIndex.executionTimeMillis - summary.afterIndex.executionTimeMillis,
        summary
      },
      null,
      2
    ) + "\n"
  );

  await client.close();
}

main().catch((err) => {
  console.error("Error en MongoDB CRUD:", err);
  process.exit(1);
});
