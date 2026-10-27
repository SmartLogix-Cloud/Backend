import "./instrument.js"; // Sentry/GlitchTip: debe ir primero para instrumentar Express.

import express from "express";
import morgan from "morgan";
import * as Sentry from "@sentry/node";

import { env } from "./config/env.js";
import healthRouter from "./routes/health.js";
import docsRouter from "./routes/docs.js";
import checkoutRouter from "./routes/checkout.js";
import ordersRouter from "./routes/orders.js";
import dashboardRouter from "./routes/dashboard.js";
import inventoryRouter from "./routes/inventory.js";
import demoRouter from "./routes/demo.js";
import proxyRouter from "./routes/proxy.js";
import { errorHandler, notFound } from "./middleware/errorHandler.js";
import { requireAuth, authorizeResource, requireRole } from "./middleware/auth.js";

const app = express();

app.use(morgan("combined"));
app.use(express.json());

// Rutas públicas: health (probes) y documentación. NO requieren token.
app.use(healthRouter);
app.use(docsRouter);

// A partir de aquí, TODO exige un JWT válido de Entra (firma/issuer/audience/exp).
app.use(requireAuth);

// Autorización por rol según el recurso y el método (Read/Write).
app.use("/inventory", authorizeResource("Inventory"));
app.use("/orders", authorizeResource("Orders"));
app.use("/shipments", authorizeResource("Shipments"));
app.use("/users", authorizeResource("Users"));
app.use("/checkout", requireRole("Orders.Write"));
app.use("/dashboard", requireRole("Orders.Read", "Orders.Write"));

// Endpoints propios del BFF (compuestos) van antes del proxy.
app.use(checkoutRouter);
app.use(ordersRouter);
app.use(dashboardRouter);
app.use(inventoryRouter);
app.use(demoRouter);

// Passthrough genérico para CRUD simple de los MS.
app.use(proxyRouter);

app.use(notFound);

// Captura de errores en GlitchTip (antes del handler propio). No-op si no hay DSN.
Sentry.setupExpressErrorHandler(app);

app.use(errorHandler);

app.listen(env.PORT, "0.0.0.0", () => {
  console.log(`BFF listening on :${env.PORT}`);
  console.log(`  -> inventory: ${env.MS_INVENTORY_URL}`);
  console.log(`  -> order:     ${env.MS_ORDER_URL}`);
  console.log(`  -> shipping:  ${env.MS_SHIPPING_URL}`);
  console.log(`  -> user:      ${env.MS_USER_URL}`);
  console.log(`  -> auth:      Microsoft Entra ID (issuer ${env.AZURE_ISSUER || "sin configurar"})`);
  console.log(`  -> docs:      http://localhost:${env.PORT}/docs`);
});
