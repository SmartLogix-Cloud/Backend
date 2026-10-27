import { Router, Request } from "express";
import { ClientRequest } from "http";
import { createProxyMiddleware } from "http-proxy-middleware";
import { env } from "../config/env.js";

const router = Router();

// express.json() ya parseó req.body — el stream HTTP quedó vacío y el proxy haría timeout.
// Re-serializamos el body parseado para que el upstream lo reciba.
const restreamBody = (proxyReq: ClientRequest, req: Request) => {
  if (!req.body || !Object.keys(req.body as object).length) return;
  const ct = (proxyReq.getHeader("content-type") || "").toString();
  if (!ct.includes("application/json")) return;
  const data = JSON.stringify(req.body);
  proxyReq.setHeader("content-length", Buffer.byteLength(data));
  proxyReq.write(data);
};

router.use(
  "/inventory",
  createProxyMiddleware({
    target: env.MS_INVENTORY_URL,
    changeOrigin: true,
    pathRewrite: { "^/inventory": "" },
    on: { proxyReq: restreamBody },
  })
);

// Express strippea el prefix antes de pasar al middleware: /orders → /, /orders/123 → /123.
// Re-agregamos el prefix usando req.originalUrl para evitar trailing slashes (Spring Boot 4
// no hace match de trailing slash por defecto: /orders OK, /orders/ → 404).
// /inventory pasa tal cual porque el MS expone /products, /warehouses, /stock (sin prefix común).
router.use(
  "/orders",
  createProxyMiddleware({
    target: env.MS_ORDER_URL,
    changeOrigin: true,
    pathRewrite: (_path, req) => req.originalUrl,
    on: { proxyReq: restreamBody },
  })
);

router.use(
  "/shipments",
  createProxyMiddleware({
    target: env.MS_SHIPPING_URL,
    changeOrigin: true,
    pathRewrite: (_path, req) => req.originalUrl,
    on: { proxyReq: restreamBody },
  })
);

router.use(
  "/users",
  createProxyMiddleware({
    target: env.MS_USER_URL,
    changeOrigin: true,
    pathRewrite: (_path, req) => req.originalUrl,
    on: { proxyReq: restreamBody },
  })
);

// Nota: /auth se eliminó. La autenticación ahora la provee Microsoft Entra ID
// (IDaaS) vía MSAL en el frontend; el BFF ya no delega login a ms-auth.

export default router;
