-- ============================================================
-- db-pedido · nuevos estados de pedido (EP3)
-- Renombra PENDIENTE -> PENDIENTE_PREPARACION, retira ENVIADO/ENTREGADO
-- e incorpora FINALIZADO e INCIDENCIA.
-- ============================================================

-- 1) Quitar los CHECK antiguos (nombres por defecto de Postgres)
ALTER TABLE pedidos          DROP CONSTRAINT IF EXISTS pedidos_estado_check;
ALTER TABLE pedido_historial DROP CONSTRAINT IF EXISTS pedido_historial_estado_anterior_check;
ALTER TABLE pedido_historial DROP CONSTRAINT IF EXISTS pedido_historial_estado_nuevo_check;

-- 2) Ensanchar columnas (PENDIENTE_PREPARACION = 21 caracteres)
ALTER TABLE pedidos          ALTER COLUMN estado TYPE VARCHAR(30);
ALTER TABLE pedido_historial ALTER COLUMN estado_anterior TYPE VARCHAR(30);
ALTER TABLE pedido_historial ALTER COLUMN estado_nuevo    TYPE VARCHAR(30);

-- 3) Migrar datos existentes al nuevo vocabulario
UPDATE pedidos SET estado = CASE estado
    WHEN 'PENDIENTE' THEN 'PENDIENTE_PREPARACION'
    WHEN 'ENVIADO'   THEN 'EN_PREPARACION'
    WHEN 'ENTREGADO' THEN 'FINALIZADO'
    ELSE estado END;

UPDATE pedido_historial SET estado_anterior = CASE estado_anterior
    WHEN 'PENDIENTE' THEN 'PENDIENTE_PREPARACION'
    WHEN 'ENVIADO'   THEN 'EN_PREPARACION'
    WHEN 'ENTREGADO' THEN 'FINALIZADO'
    ELSE estado_anterior END;

UPDATE pedido_historial SET estado_nuevo = CASE estado_nuevo
    WHEN 'PENDIENTE' THEN 'PENDIENTE_PREPARACION'
    WHEN 'ENVIADO'   THEN 'EN_PREPARACION'
    WHEN 'ENTREGADO' THEN 'FINALIZADO'
    ELSE estado_nuevo END;

-- 4) Nuevo valor por defecto
ALTER TABLE pedidos ALTER COLUMN estado SET DEFAULT 'PENDIENTE_PREPARACION';

-- 5) Nuevos CHECK con el vocabulario vigente
ALTER TABLE pedidos ADD CONSTRAINT pedidos_estado_check
    CHECK (estado IN ('PENDIENTE_PREPARACION','APROBADO','RECHAZADO',
                      'EN_PREPARACION','CANCELADO','FINALIZADO','INCIDENCIA'));

ALTER TABLE pedido_historial ADD CONSTRAINT pedido_historial_estado_anterior_check
    CHECK (estado_anterior IS NULL OR estado_anterior IN (
           'PENDIENTE_PREPARACION','APROBADO','RECHAZADO',
           'EN_PREPARACION','CANCELADO','FINALIZADO','INCIDENCIA'));

ALTER TABLE pedido_historial ADD CONSTRAINT pedido_historial_estado_nuevo_check
    CHECK (estado_nuevo IN (
           'PENDIENTE_PREPARACION','APROBADO','RECHAZADO',
           'EN_PREPARACION','CANCELADO','FINALIZADO','INCIDENCIA'));
