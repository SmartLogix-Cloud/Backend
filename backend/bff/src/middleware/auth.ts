import { Request, Response, NextFunction } from "express";
import jwt, { JwtHeader, SigningKeyCallback } from "jsonwebtoken";
import { JwksClient } from "jwks-rsa";
import { env } from "../config/env.js";

/**
 * Validación de JWT emitidos por Microsoft Entra ID (IDaaS).
 *
 * Cubre el indicador de la pauta: el BFF valida firma, issuer, audience y
 * vigencia del token, y autoriza por rol, respondiendo 401/403 según corresponda.
 */

// Cliente JWKS: descarga y cachea las claves públicas de Entra (rotación incluida).
const jwks = new JwksClient({
  jwksUri: env.AZURE_JWKS_URI,
  cache: true,
  cacheMaxAge: 24 * 60 * 60 * 1000, // 24h
  rateLimit: true,
  jwksRequestsPerMinute: 10,
});

// Resuelve la clave pública (por `kid`) para verificar la firma RS256.
function getKey(header: JwtHeader, callback: SigningKeyCallback) {
  if (!header.kid) {
    callback(new Error("Token sin 'kid' en el header"));
    return;
  }
  jwks.getSigningKey(header.kid, (err, key) => {
    if (err || !key) {
      callback(err ?? new Error("No se encontró la clave de firma"));
      return;
    }
    callback(null, key.getPublicKey());
  });
}

/** Payload esperado tras validar el token de Entra. */
export interface EntraJwtPayload extends jwt.JwtPayload {
  roles?: string[];
  scp?: string;
  name?: string;
  preferred_username?: string;
  oid?: string;
}

// Adjuntamos el usuario autenticado al request para uso aguas abajo.
declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace Express {
    interface Request {
      user?: EntraJwtPayload;
    }
  }
}

function unauthorized(res: Response, detail: string) {
  return res.status(401).json({
    status: 401,
    title: "No autenticado",
    detail,
    timestamp: new Date().toISOString(),
  });
}

/**
 * Middleware: exige un access token válido de Entra.
 * Verifica firma (RS256 vía JWKS), issuer, audience y expiración.
 */
export function requireAuth(req: Request, res: Response, next: NextFunction) {
  const authHeader = req.headers.authorization ?? "";
  const [scheme, token] = authHeader.split(" ");

  if (scheme !== "Bearer" || !token) {
    return unauthorized(res, "Falta el header Authorization: Bearer <token>");
  }

  jwt.verify(
    token,
    getKey,
    {
      algorithms: ["RS256"],
      issuer: env.AZURE_ISSUER,
      audience: env.AZURE_API_AUDIENCE,
    },
    (err, decoded) => {
      if (err) {
        if (err.name === "TokenExpiredError") {
          return unauthorized(res, "El token ha expirado");
        }
        return unauthorized(res, `Token inválido: ${err.message}`);
      }
      req.user = decoded as EntraJwtPayload;
      next();
    }
  );
}

/** Roles (App Roles) del token, claim `roles`. */
function rolesOf(user?: EntraJwtPayload): string[] {
  return user?.roles ?? [];
}

/** Scopes del token, claim `scp` (separados por espacio). */
function scopesOf(user?: EntraJwtPayload): string[] {
  return user?.scp ? user.scp.split(" ").filter(Boolean) : [];
}

/**
 * Middleware: exige que el token tenga al menos uno de los roles/scopes indicados.
 * Debe usarse DESPUÉS de requireAuth. Responde 403 si no cumple.
 */
export function requireRole(...allowed: string[]) {
  return (req: Request, res: Response, next: NextFunction) => {
    const granted = new Set([...rolesOf(req.user), ...scopesOf(req.user)]);
    const ok = allowed.some((r) => granted.has(r));
    if (!ok) {
      return res.status(403).json({
        status: 403,
        title: "Acceso denegado",
        detail: `Se requiere uno de los roles: ${allowed.join(", ")}`,
        timestamp: new Date().toISOString(),
      });
    }
    next();
  };
}

/**
 * Autorización por recurso según el método HTTP:
 * GET/HEAD -> <Recurso>.Read ; mutaciones -> <Recurso>.Write.
 * Ej: authorizeResource("Inventory") protege /inventory con Inventory.Read/Write.
 */
export function authorizeResource(resource: string) {
  return (req: Request, res: Response, next: NextFunction) => {
    const isRead = req.method === "GET" || req.method === "HEAD";
    const required = isRead ? `${resource}.Read` : `${resource}.Write`;
    return requireRole(required, `${resource}.Write`)(req, res, next);
  };
}
