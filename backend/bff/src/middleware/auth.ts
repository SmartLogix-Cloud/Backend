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
 * Matriz RBAC perfil -> permisos (EP2).
 *
 * Entra ID emite en el claim `roles` un PERFIL grueso (Admin/Operador/Cliente) y
 * es la aplicación (este BFF) la que lo expande a permisos finos. Es el patrón
 * recomendado por Microsoft: el IdP define roles de negocio, la app define el
 * vocabulario de permisos. Jerarquía: Admin ⊇ Operador ⊇ Cliente.
 */
const CLIENTE_PERMS = ["Inventory.Read", "Orders.Read", "Shipments.Read"];
const OPERADOR_PERMS = [
  ...CLIENTE_PERMS,
  "Inventory.Write",
  "Orders.Write",
  "Shipments.Write",
];
const ADMIN_PERMS = [
  ...OPERADOR_PERMS,
  "Users.Read",
  "Users.Write",
  "Admin", // administración de la topología de RabbitMQ (/api/admin/*)
];

const PROFILE_PERMISSIONS: Record<string, string[]> = {
  Cliente: CLIENTE_PERMS,
  Operador: OPERADOR_PERMS,
  Admin: ADMIN_PERMS,
};

/**
 * Permisos efectivos del usuario. Expande los perfiles del claim `roles` según
 * la matriz; además conserva compatibilidad con tokens que aún traigan permisos
 * finos (p. ej. "Inventory.Read") o scopes delegados (`scp`).
 */
function grantedPermissions(user?: EntraJwtPayload): Set<string> {
  const granted = new Set<string>();
  for (const role of rolesOf(user)) {
    const perms = PROFILE_PERMISSIONS[role];
    if (perms) perms.forEach((p) => granted.add(p));
    granted.add(role); // compatibilidad con roles finos legados
  }
  scopesOf(user).forEach((s) => granted.add(s));
  return granted;
}

/**
 * Middleware: exige que el usuario tenga al menos uno de los permisos indicados
 * (resueltos a partir de su perfil). Debe usarse DESPUÉS de requireAuth.
 * Responde 403 si no cumple.
 */
export function requireRole(...allowed: string[]) {
  return (req: Request, res: Response, next: NextFunction) => {
    const granted = grantedPermissions(req.user);
    const ok = allowed.some((r) => granted.has(r));
    if (!ok) {
      return res.status(403).json({
        status: 403,
        title: "Acceso denegado",
        detail: `Se requiere uno de los permisos: ${allowed.join(", ")}`,
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
