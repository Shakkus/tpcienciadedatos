// COMPARATIVA SQL vs NOSQL (Documentos):
// En un modelo relacional tradicional (SQL), representar un pase de movilidad
// requeriría al menos 3 tablas conectadas por JOINs: "Pase", "PreciosPorCiudad"
// y "Tags", lo cual penaliza el rendimiento en lecturas de alta concurrencia.
// En MongoDB (NoSQL), al usar el patrón de "Embedding" (documentos anidados),
// toda la información del catálogo se lee en una sola operación atómica a disco.
// Esto elimina la latencia de las uniones, optimizando la lectura del catálogo
// comercial en tiempo real, ideal para la capa Polyglot de NexoUrbano.

use('nexourbano');

// 1. Insertar >= 30 documentos (Generamos un bucle para cumplir el mínimo)
let docs = [];
let ciudades = ["Buenos Aires", "Mendoza", "Córdoba", "Rosario"];
let categorias = ["Pase", "Recarga", "Casco", "Seguro"];

for (let i = 1; i <= 32; i++) {
    docs.push({
        sku: "SKU00" + i,
        nombre: "Nexo " + categorias[i % 4] + " " + i,
        categoria: categorias[i % 4],
        ciudad: ciudades[i % 4],
        tags: ["eco", i % 2 === 0 ? "premium" : "basico"],
        detalles: {
            precio: 1000 + (i * 50),
            stock_disponible: 50 + i,
            activo: true
        }
    });
}
db.productos.insertMany(docs);
print("Insertados 32 documentos con éxito.");

// 2. Operaciones CRUD Básicas
print("--- TEST FIND ---");
printjson(db.productos.find({ categoria: "Pase" }).limit(1).toArray());

print("--- TEST UPDATE ---");
db.productos.updateOne({ sku: "SKU001" }, { $set: { "detalles.precio": 1500 } });
print("Precio actualizado para SKU001.");

print("--- TEST DELETE ---");
db.productos.deleteOne({ sku: "SKU002" });
print("SKU002 eliminado del catálogo.");

// 3. Índices y Explain
print("--- EXPLAIN ANTES DEL ÍNDICE ---");
printjson(db.productos.find({ ciudad: "Mendoza", categoria: "Pase" }).explain("executionStats").executionStats);

// Crear índice compuesto
db.productos.createIndex({ ciudad: 1, categoria: 1 });
print("Índice compuesto {ciudad: 1, categoria: 1} creado.");

print("--- EXPLAIN DESPUÉS DEL ÍNDICE ---");
printjson(db.productos.find({ ciudad: "Mendoza", categoria: "Pase" }).explain("executionStats").executionStats);