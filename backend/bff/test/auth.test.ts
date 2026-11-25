import { describe, it, before } from "node:test";
import assert from "node:assert/strict";
import type { Request, Response, NextFunction } from "express";

// Config de Entra ficticia: los tests no llaman a la red (los casos de token
// inválido fallan antes de pedir la clave pública al JWKS).
process.env.AZURE_ISSUER = "https://login.microsoftonline.com/tenant-test/v2.0";
process.env.AZURE_API_AUDIENCE = "api-test";
process.env.AZURE_JWKS_URI = "https://login.microsoftonline.com/tenant-test/discovery/v2.0/keys";

let auth: typeof import("../src/middleware/auth.js");

before(async () => {
  auth = await import("../src/middleware/auth.js");
});

/** Ejecuta un middleware con un request/response simulados. */
function run(
  mw: (req: Request, res: Response, next: NextFunction) => unknown,
  req: Partial<Request>,
): Promise<{ status?: number; body?: any; nextCalled: boolean }> {
  return new Promise((resolve) => {
    const result: { status?: number; body?: any; nextCalled: boolean } = { nextCalled: false };
    const res = {
      status(code: number) {
        result.status = code;
        return this;
      },
      json(body: unknown) {
        result.body = body;
        resolve(result);
        return this;
      },
    } as unknown as Response;
    mw({ headers: {}, method: "GET", ...req } as Request, res, () => {
      result.nextCalled = true;
      resolve(result);
    });
  });
}

const withRoles = (roles: string[], method = "GET"): Partial<Request> => ({ method, user: { roles } });

describe("requireRole: matriz de perfiles de Entra -> permisos", () => {
  it("Admin puede leer y escribir pedidos", async () => {
    assert.equal((await run(auth.requireRole("Orders.Read"), withRoles(["Admin"]))).nextCalled, true);
    assert.equal((await run(auth.requireRole("Orders.Write"), withRoles(["Admin"]))).nextCalled, true);
  });

  it("Admin puede administrar RabbitMQ (/api/admin)", async () => {
    assert.equal((await run(auth.requireRole("Admin"), withRoles(["Admin"]))).nextCalled, true);
  });

  it("Operador escribe pedidos pero no administra RabbitMQ", async () => {
    assert.equal((await run(auth.requireRole("Orders.Write"), withRoles(["Operador"]))).nextCalled, true);
    const r = await run(auth.requireRole("Admin"), withRoles(["Operador"]));
    assert.equal(r.nextCalled, false);
    assert.equal(r.status, 403);
  });

  it("Cliente solo lee", async () => {
    assert.equal((await run(auth.requireRole("Orders.Read"), withRoles(["Cliente"]))).nextCalled, true);
    const r = await run(auth.requireRole("Orders.Write"), withRoles(["Cliente"]));
    assert.equal(r.status, 403);
    assert.match(r.body.detail, /Orders\.Write/);
  });

  it("sin roles ni scopes responde 403", async () => {
    const r = await run(auth.requireRole("Orders.Read"), withRoles([]));
    assert.equal(r.status, 403);
  });

  it("un perfil desconocido no otorga permisos", async () => {
    const r = await run(auth.requireRole("Orders.Read"), withRoles(["Invitado"]));
    assert.equal(r.status, 403);
  });

  it("acepta permisos finos legados en el claim roles", async () => {
    assert.equal((await run(auth.requireRole("Inventory.Read"), withRoles(["Inventory.Read"]))).nextCalled, true);
  });

  it("acepta scopes delegados (scp)", async () => {
    const r = await run(auth.requireRole("Orders.Read"), { user: { scp: "openid Orders.Read" } });
    assert.equal(r.nextCalled, true);
  });
});

describe("authorizeResource: Read para GET, Write para mutaciones", () => {
  it("Cliente puede hacer GET /orders", async () => {
    assert.equal((await run(auth.authorizeResource("Orders"), withRoles(["Cliente"], "GET"))).nextCalled, true);
  });

  it("Cliente no puede hacer POST /orders", async () => {
    const r = await run(auth.authorizeResource("Orders"), withRoles(["Cliente"], "POST"));
    assert.equal(r.status, 403);
  });

  it("Operador puede hacer PATCH /shipments", async () => {
    assert.equal((await run(auth.authorizeResource("Shipments"), withRoles(["Operador"], "PATCH"))).nextCalled, true);
  });
});

describe("requireAuth: rechaza peticiones sin token válido", () => {
  it("sin header Authorization responde 401", async () => {
    const r = await run(auth.requireAuth, {});
    assert.equal(r.status, 401);
    assert.equal(r.nextCalled, false);
  });

  it("con esquema distinto de Bearer responde 401", async () => {
    const r = await run(auth.requireAuth, { headers: { authorization: "Basic abc" } });
    assert.equal(r.status, 401);
  });

  it("con un token mal formado responde 401", async () => {
    const r = await run(auth.requireAuth, { headers: { authorization: "Bearer abc.def.ghi" } });
    assert.equal(r.status, 401);
    assert.match(r.body.detail, /Token inválido/);
  });
});
