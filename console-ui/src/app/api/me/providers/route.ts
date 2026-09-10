import { NextRequest } from "next/server";
import { coordinatorUrl, missingPrivyToken, privyAuth, relayJSON } from "@/lib/server/coordinator";

export async function GET(req: NextRequest) {
  const authHeader = privyAuth(req);
  if (!authHeader) return missingPrivyToken();

  const res = await fetch(`${coordinatorUrl()}/v1/me/providers`, {
    headers: { Authorization: authHeader },
    cache: "no-store",
  });
  return relayJSON(res, { errorFallback: true });
}
