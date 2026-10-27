-- ============================================================
-- db-envio · nuevos estados de envío (EP3)
-- Reemplaza CREADO/ASIGNADO/EN_RUTA/INCIDENCIA por el nuevo ciclo:
-- LISTO_PARA_RETIRO, RETIRADO, EN_TRANSITO, ENTREGADO, PERDIDO,
-- RETRASADO, ENTREGA_FALLIDA.
-- ============================================================

-- 1) Quitar los CHECK antiguos
ALTER TABLE envios            DROP CONSTRAINT IF EXISTS envios_estado_check;
ALTER TABLE envio_seguimiento DROP CONSTRAINT IF EXISTS envio_seguimiento_estado_check;

-- 2) Ensanchar columnas (LISTO_PARA_RETIRO = 17, margen a 30)
ALTER TABLE envios            ALTER COLUMN estado TYPE VARCHAR(30);
ALTER TABLE envio_seguimiento ALTER COLUMN estado TYPE VARCHAR(30);

-- 3) Migrar datos existentes al nuevo vocabulario
UPDATE envios SET estado = CASE estado
    WHEN 'CREADO'     THEN 'LISTO_PARA_RETIRO'
    WHEN 'ASIGNADO'   THEN 'RETIRADO'
    WHEN 'EN_RUTA'    THEN 'EN_TRANSITO'
    WHEN 'INCIDENCIA' THEN 'RETRASADO'
    ELSE estado END;

UPDATE envio_seguimiento SET estado = CASE estado
    WHEN 'CREADO'     THEN 'LISTO_PARA_RETIRO'
    WHEN 'ASIGNADO'   THEN 'RETIRADO'
    WHEN 'EN_RUTA'    THEN 'EN_TRANSITO'
    WHEN 'INCIDENCIA' THEN 'RETRASADO'
    ELSE estado END;

-- 4) Nuevo valor por defecto
ALTER TABLE envios ALTER COLUMN estado SET DEFAULT 'LISTO_PARA_RETIRO';

-- 5) Nuevos CHECK con el vocabulario vigente
ALTER TABLE envios ADD CONSTRAINT envios_estado_check
    CHECK (estado IN ('LISTO_PARA_RETIRO','RETIRADO','EN_TRANSITO',
                      'ENTREGADO','PERDIDO','RETRASADO','ENTREGA_FALLIDA'));

ALTER TABLE envio_seguimiento ADD CONSTRAINT envio_seguimiento_estado_check
    CHECK (estado IN ('LISTO_PARA_RETIRO','RETIRADO','EN_TRANSITO',
                      'ENTREGADO','PERDIDO','RETRASADO','ENTREGA_FALLIDA'));
