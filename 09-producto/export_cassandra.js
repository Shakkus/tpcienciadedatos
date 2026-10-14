const cassandra = require('cassandra-driver');
const fs = require('fs');
const path = require('path');

const client = new cassandra.Client({
  contactPoints: ['127.0.0.1'],
  localDataCenter: 'datacenter1', 
  keyspace: 'nexo'
});

async function exportarUltimaLectura() {
  try {
    const query = 'SELECT vehicle_id, ts, battery_pct, status FROM lecturas_vehiculo PER PARTITION LIMIT 1;';
    const result = await client.execute(query);

    const datosExport = result.rows.map(r => ({
      vehicle_id: r.vehicle_id,
      ts: r.ts.toISOString(),
      battery_pct: r.battery_pct,
      status: r.status
    }));

    const dir = '09-producto';
    if (!fs.existsSync(dir)){
        fs.mkdirSync(dir);
    }

    const rutaSalida = path.join(dir, 'cassandra_ultima_lectura.json');
    fs.writeFileSync(rutaSalida, JSON.stringify(datosExport, null, 4), 'utf8');

    console.log(`Exportación exitosa: ${datosExport.length} vehículos guardados en ${rutaSalida}`);
  } catch (error) {
    console.error("Error exportando desde Cassandra:", error);
  } finally {
    await client.shutdown();
  }
}

exportarUltimaLectura();
