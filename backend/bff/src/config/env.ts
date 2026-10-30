export const env = {
  PORT: Number(process.env.PORT) || 3000,
  MS_INVENTORY_URL: process.env.MS_INVENTORY_URL || "http://ms-inventory:8080",
  MS_ORDER_URL: process.env.MS_ORDER_URL || "http://ms-order:8080",
  MS_SHIPPING_URL: process.env.MS_SHIPPING_URL || "http://ms-shipping:8080",
  MS_USER_URL: process.env.MS_USER_URL || "http://ms-user:8080",
  // Microservicio administrador de RabbitMQ (topología: colas/exchanges/bindings).
  MS_BROKER_ADMIN_URL: process.env.MS_BROKER_ADMIN_URL || "http://ms-broker-admin:8080",
  HTTP_TIMEOUT_MS: Number(process.env.HTTP_TIMEOUT_MS) || 5000,
  // Circuit Breaker: fallos consecutivos para abrir y ventana antes de reintentar.
  CB_FAILURE_THRESHOLD: Number(process.env.CB_FAILURE_THRESHOLD) || 5,
  CB_RESET_TIMEOUT_MS: Number(process.env.CB_RESET_TIMEOUT_MS) || 15000,
  GLITCHTIP_DSN: process.env.BFF_GLITCHTIP_DSN || "",
  NODE_ENV: process.env.NODE_ENV || "development",

  // --- Microsoft Entra ID (validación de JWT) ---
  AZURE_TENANT_ID: process.env.AZURE_TENANT_ID || "",
  // Audience aceptada: el client-id de la API y/o el App ID URI.
  AZURE_API_AUDIENCE: process.env.AZURE_API_AUDIENCE || "",
  AZURE_ISSUER:
    process.env.AZURE_ISSUER ||
    (process.env.AZURE_TENANT_ID
      ? `https://login.microsoftonline.com/${process.env.AZURE_TENANT_ID}/v2.0`
      : ""),
  AZURE_JWKS_URI:
    process.env.AZURE_JWKS_URI ||
    (process.env.AZURE_TENANT_ID
      ? `https://login.microsoftonline.com/${process.env.AZURE_TENANT_ID}/discovery/v2.0/keys`
      : ""),
};
