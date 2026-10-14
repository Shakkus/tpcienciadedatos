const fs = require("fs");
const path = require("path");
const { MongoClient } = require("mongodb");

const uri = "mongodb://127.0.0.1:27018";
const dbName = "nexo";
const csvPath = path.join(__dirname, "..", "data", "ventas.csv");
const outputPath = path.join(__dirname, "..", "09-producto", "ventas_agg.json");
const periodo = "2026-Q2";
const filtroPeriodo = {
  ts: {
    $gte: new Date("2026-04-01T00:00:00.000Z"),
    $lt: new Date("2026-07-01T00:00:00.000Z")
  }
};

function parseCsvLine(line) {
  const result = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];

    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === "," && !inQuotes) {
      result.push(current);
      current = "";
    } else {
      current += char;
    }
  }

  result.push(current);
  return result.map((v) => v.trim());
}

function loadVentasCsv(filePath) {
  const text = fs.readFileSync(filePath, "utf8").trim();
  const lines = text.split(/\r?\n/).filter(Boolean);

  if (lines.length < 2) {
    throw new Error("El CSV no tiene datos suficientes.");
  }

  const headers = parseCsvLine(lines[0]);
  const rows = lines.slice(1).map((line) => {
    const values = parseCsvLine(line);
    const obj = {};

    headers.forEach((header, index) => {
      obj[header] = values[index] ?? "";
    });

    return {
      venta_id: obj.venta_id,
      ts: new Date(obj.ts).toISOString(),
      sku: obj.sku,
      categoria: obj.categoria,
      rider_id: obj.rider_id,
      ciudad: obj.ciudad,
      monto_ars: Number(obj.monto_ars),
      canal: obj.canal
    };
  });

  return rows;
}

async function main() {
  const client = new MongoClient(uri);
  await client.connect();

  const db = client.db(dbName);
  const ventas = db.collection("ventas");

  const docs = loadVentasCsv(csvPath);

  await ventas.deleteMany({});
  await ventas.insertMany(docs);

  const totalVentasCargadas = await ventas.countDocuments(filtroPeriodo);

  const ventasPorCategoria = await ventas.aggregate([
    { $match: filtroPeriodo },
    {
      $lookup: {
        from: "productos",
        localField: "sku",
        foreignField: "sku",
        as: "producto"
      }
    },
    {
      $addFields: {
        producto: { $arrayElemAt: ["$producto", 0] }
      }
    },
    {
      $group: {
        _id: "$categoria",
        totalVentas: { $sum: "$monto_ars" },
        ventas: { $sum: 1 }
      }
    },
    {
      $sort: { totalVentas: -1 }
    },
    {
      $project: {
        _id: 0,
        categoria: "$_id",
        totalVentas: 1,
        ventas: 1
      }
    }
  ]).toArray();

  const top5Productos = await ventas.aggregate([
    { $match: filtroPeriodo },
    {
      $lookup: {
        from: "productos",
        localField: "sku",
        foreignField: "sku",
        as: "producto"
      }
    },
    {
      $addFields: {
        producto: { $arrayElemAt: ["$producto", 0] }
      }
    },
    {
      $group: {
        _id: "$sku",
        nombre: { $first: "$producto.nombre" },
        categoria: { $first: "$producto.categoria" },
        totalVentas: { $sum: "$monto_ars" },
        ventas: { $sum: 1 }
      }
    },
    {
      $sort: { totalVentas: -1 }
    },
    {
      $limit: 5
    },
    {
      $project: {
        _id: 0,
        sku: "$_id",
        nombre: 1,
        categoria: 1,
        totalVentas: 1,
        ventas: 1
      }
    }
  ]).toArray();

  const ventasPorMes = await ventas.aggregate([
    { $match: filtroPeriodo },
    {
      $group: {
        _id: {
          $dateToString: {
            format: "%Y-%m",
            date: { $toDate: "$ts" }
          }
        },
        totalVentas: { $sum: "$monto_ars" },
        ventas: { $sum: 1 }
      }
    },
    { $sort: { _id: 1 } },
    {
      $project: {
        _id: 0,
        mes: "$_id",
        totalVentas: 1,
        ventas: 1
      }
    }
  ]).toArray();

  const resultado = {
    periodo,
    totalVentasCargadas,
    ventasPorCategoria,
    top5Productos,
    ventasPorMes
  };

  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, JSON.stringify(resultado, null, 2) + "\n");

  console.log(JSON.stringify(resultado, null, 2));

  await client.close();
}

main().catch((error) => {
  console.error("Error en M11 Mongo:", error);
  process.exit(1);
});
