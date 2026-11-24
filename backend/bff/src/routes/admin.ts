import { Router, Request } from "express";
import { ClientRequest } from "http";
import { createProxyMiddleware } from "http-proxy-middleware";
import { env } from "../config/env.js";

/**
 * Proxy de administración de RabbitMQ (EP2).
 *
 * Expone la API del microservicio ms-broker-admin bajo /api/admin/*. El control
 * de acceso (solo rol Admin) se aplica en server.ts ANTES de montar este router,
 * de modo que un usuario sin el perfil Admin recibe 403 y nunca llega al MS
 * (cumple la prueba P9 de la guía: "administrador con un usuario que no es Admin").
 *
 * El prefijo /api/admin se reescribe a la raíz del ms-broker-admin, que expone
 * /queues, /exchanges y /bindings.
 */
const router = Router();

// express.json() ya consumió el body: lo re-serializamos hacia el upstream.
const restreamBody = (proxyReq: ClientRequest, req: Request) => {
  if (!req.body || !Object.keys(req.body as object).length) return;
  const ct = (proxyReq.getHeader("content-type") || "").toString();
  if (!ct.includes("application/json")) return;
  const data = JSON.stringify(req.body);
  proxyReq.setHeader("content-length", Buffer.byteLength(data));
  proxyReq.write(data);
};

router.use(
  "/api/admin",
  createProxyMiddleware({
    target: env.MS_BROKER_ADMIN_URL,
    changeOrigin: true,
    pathRewrite: { "^/api/admin": "" },
    on: { proxyReq: restreamBody },
  })
);

export default router;
