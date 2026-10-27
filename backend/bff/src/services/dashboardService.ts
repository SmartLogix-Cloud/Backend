import { order } from "../clients/order.js";
import { inventory } from "../clients/inventory.js";
import { shipping } from "../clients/shipping.js";

const ORDER_STATUSES = ["PENDIENTE_PREPARACION", "APROBADO", "RECHAZADO", "EN_PREPARACION", "FINALIZADO", "CANCELADO", "INCIDENCIA"];

export async function dashboard() {
  const [ordersByStatus, lowStock, shipmentsInTransit] = await Promise.all([
    ordersByStatusMap(),
    inventory.lowStock().catch(() => []),
    shipping.list("EN_TRANSITO").catch(() => []),
  ]);

  return {
    orders: ordersByStatus,
    lowStock: {
      total: lowStock.length,
      items: lowStock.slice(0, 10),
    },
    shipmentsInTransit: {
      total: shipmentsInTransit.length,
      items: shipmentsInTransit.slice(0, 10),
    },
    generatedAt: new Date().toISOString(),
  };
}

async function ordersByStatusMap() {
  const entries = await Promise.all(
    ORDER_STATUSES.map((status) =>
      order.list(status)
        .then((arr) => [status, arr.length])
        .catch(() => [status, null])
    )
  );
  return Object.fromEntries(entries as any);
}
