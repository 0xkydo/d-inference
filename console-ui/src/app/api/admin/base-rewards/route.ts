import { NextRequest } from "next/server";
import { coordinatorUrl, missingPrivyToken, privyAuth, relayJSON } from "@/lib/server/coordinator";

// Proxy for the admin-only base-rewards status endpoint. Forwards the caller's
// Privy bearer token (or the privy-token cookie) to the coordinator, which does
// the actual admin authorization. Read-only.
export async function GET(req: NextRequest) {
  const authHeader = privyAuth(req);
  if (!authHeader) return missingPrivyToken();

  const res = await fetch(`${coordinatorUrl()}/v1/admin/base-rewards`, {
    headers: { Authorization: authHeader },
    cache: "no-store",
  });
  return relayJSON(res, { errorFallback: true });
}
