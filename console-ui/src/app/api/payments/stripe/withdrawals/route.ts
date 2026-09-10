import { NextRequest } from "next/server";
import { coordinatorUrl, privyAuth, relayJSON } from "@/lib/server/coordinator";

// Proxy for GET /v1/billing/stripe/withdrawals — recent payout history for
// the Billing page.

export async function GET(req: NextRequest) {
  const authHeader = privyAuth(req);

  const url = new URL(req.url);
  const limit = url.searchParams.get("limit");
  const upstream = `${coordinatorUrl()}/v1/billing/stripe/withdrawals${limit ? `?limit=${limit}` : ""}`;

  const res = await fetch(upstream, {
    headers: { ...(authHeader ? { Authorization: authHeader } : {}) },
  });
  return relayJSON(res, { lenientBody: true });
}
