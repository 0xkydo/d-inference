import { NextRequest } from "next/server";
import { coordinatorUrl, privyAuth, relayJSON } from "@/lib/server/coordinator";

// Proxy for POST /v1/billing/stripe/onboard. Stripe Payouts is Privy-only
// (no API-key access), so we forward the Privy session token via the cookie
// fallback when no Authorization header is present.

export async function POST(req: NextRequest) {
  const authHeader = privyAuth(req);
  const body = await req.json().catch(() => ({}));

  const res = await fetch(`${coordinatorUrl()}/v1/billing/stripe/onboard`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(authHeader ? { Authorization: authHeader } : {}),
    },
    body: JSON.stringify(body),
  });
  return relayJSON(res, { lenientBody: true });
}
