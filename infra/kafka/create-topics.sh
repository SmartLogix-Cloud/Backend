#!/usr/bin/env bash
# ===========================================================================
# Crea los topicos de SmartLogix con su configuracion de administracion.
# Idempotente: --if-not-exists permite re-ejecutarlo sin error.
#
# Justificacion de la configuracion (ver infra/kafka/README.md):
#   partitions=3          -> hasta 3 consumidores en paralelo por consumer group
#                            (1 por particion), orden garantizado por clave.
#   replication-factor=3  -> una copia en cada broker.
#   min.insync.replicas=2 -> con acks=all, una escritura se confirma cuando
#                            al menos 2 replicas la tienen: tolera perder 1 broker.
# ===========================================================================
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka1:29092,kafka2:29092,kafka3:29092}"

create_topic() {
  local name="$1"; shift
  echo ">> Creando topico ${name}"
  kafka-topics --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
    --topic "${name}" --partitions 3 --replication-factor 3 "$@"
}

# --- Eventos del ciclo de vida del pedido (ms-order -> ms-analytics) ---
# Historial completo de eventos: cleanup=delete, retencion 7 dias (replay semanal).
create_topic smartlogix.order-events \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config min.insync.replicas=2 \
  --config compression.type=producer \
  --config max.message.bytes=1048576

# --- Eventos de seguimiento de envios (ms-shipping -> ms-order, ms-analytics) ---
# Clave = id del envio. compact,delete: conserva siempre el ULTIMO estado de cada
# envio (para reconstruir proyecciones) y purga lo que supere los 30 dias.
create_topic smartlogix.shipment-events \
  --config cleanup.policy=compact,delete \
  --config retention.ms=2592000000 \
  --config min.compaction.lag.ms=60000 \
  --config segment.ms=86400000 \
  --config min.insync.replicas=2 \
  --config compression.type=producer

# --- Dead Letter Topics: eventos que fallaron tras los reintentos ---
# Retencion mas larga (14 dias) para dar tiempo a analizarlos y re-procesarlos.
for dlt in smartlogix.order-events.DLT smartlogix.shipment-events.DLT; do
  create_topic "${dlt}" \
    --config cleanup.policy=delete \
    --config retention.ms=1209600000 \
    --config min.insync.replicas=2
done

echo ">> Topicos actuales:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --describe --topic 'smartlogix\..*'
