-- Proyeccion del seguimiento de envio por pedido, alimentada por eventos Kafka
-- del topico smartlogix.shipment-events (producidos por ms-shipping).
CREATE TABLE pedido_seguimiento (
    id_pedido          BIGINT       PRIMARY KEY REFERENCES pedidos(id_pedido) ON DELETE CASCADE,
    id_envio           BIGINT       NOT NULL,
    tracking_number    VARCHAR(60),
    transportista      VARCHAR(120),
    estado_envio       VARCHAR(30)  NOT NULL,
    ultima_ubicacion   VARCHAR(255),
    ultimo_comentario  VARCHAR(500),
    ultimo_evento_id   VARCHAR(64)  NOT NULL,
    ultimo_evento_at   TIMESTAMPTZ  NOT NULL,
    eventos_recibidos  INTEGER      NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_seguimiento_envio ON pedido_seguimiento(id_envio);
