USE nexo;

-- A. Últimas 20 lecturas de un vehículo específico
SELECT * FROM lecturas_vehiculo WHERE vehicle_id = 'V0044' LIMIT 20;

-- B. Rango horario de un vehículo específico
SELECT * FROM lecturas_vehiculo 
WHERE vehicle_id = 'V0044' 
  AND ts >= '2026-05-01 00:00:00+0000' 
  AND ts <= '2026-05-01 12:00:00+0000';

-- C. ANTI-PATRÓN INTENCIONAL (Debe fallar)
SELECT * FROM lecturas_vehiculo WHERE ts >= '2026-05-01 08:00:00+0000';
