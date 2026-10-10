use('nexourbano');

// Insertar ventas de prueba para cruzar con productos
db.ventas.insertMany([
    { id_venta: 1, sku: "SKU003", cantidad: 2, fecha: new Date("2026-10-01"), monto: 3000 },
    { id_venta: 2, sku: "SKU004", cantidad: 1, fecha: new Date("2026-10-05"), monto: 1500 },
    { id_venta: 3, sku: "SKU003", cantidad: 1, fecha: new Date("2026-10-10"), monto: 1200 },
    { id_venta: 4, sku: "SKU008", cantidad: 5, fecha: new Date("2026-09-15"), monto: 5500 }
]);

const pipeline = [
    // 1. $match último mes (Octubre 2026)
    { $match: { fecha: { $gte: new Date("2026-10-01"), $lt: new Date("2026-11-01") } } },
    // 2. $lookup a productos
    {
        $lookup: {
            from: "productos",
            localField: "sku",
            foreignField: "sku",
            as: "info_producto"
        }
    },
    { $unwind: "$info_producto" },
    // 3. $group total por categoría
    {
        $group: {
            _id: "$info_producto.categoria",
            pases_vendidos: { $sum: "$cantidad" },
            ingreso_total: { $sum: "$monto" }
        }
    },
    // 4. $project del documento limpio
    {
        $project: {
            _id: 0,
            tipo_pase: "$_id",
            pases_vendidos: 1,
            ingreso_total: 1,
            precio_promedio: { $divide: ["$ingreso_total", "$pases_vendidos"] }
        }
    },
    // 5. top
    { $sort: { ingreso_total: -1 } },
    { $limit: 5 }
];

const resultado = db.ventas.aggregate(pipeline).toArray();
print(JSON.stringify(resultado, null, 2));