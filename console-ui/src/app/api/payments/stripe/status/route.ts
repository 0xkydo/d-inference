import { NextRequest } from "next/server";
import { coordinatorUrl, privyAuth, relayJSON } from "@/lib/server/coordinator";

// Proxy for GET /v1/billing/stripe/status. Forwards `?refresh=1` so the
// Billing page can request a live Stripe refresh after onboarding redirect.

export async function GET(req: NextRequest) {
  const authHeader = privyAuth(req);

  const url = new URL(req.url);
  const refresh = url.searchParams.get("refresh");
  const upstream = `${coordinatorUrl()}/v1/billing/stripe/status${refresh ? `?refresh=${refresh}` : ""}`;

  const res = await fetch(upstream, {
    headers: { ...(authHeader ? { Authorization: authHeader } : {}) },
  });
  return relayJSON(res, { lenientBody: true });
}
