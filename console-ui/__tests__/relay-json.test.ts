import { describe, expect, it } from "vitest";
import { relayJSON } from "@/lib/server/coordinator";

describe("relayJSON", () => {
  it("passes through an OK JSON response", async () => {
    const response = await relayJSON(new Response(JSON.stringify({ ok: true })));

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({ ok: true });
  });

  it("relays non-OK response text", async () => {
    const response = await relayJSON(new Response("backend failed", { status: 502 }));

    expect(response.status).toBe(502);
    await expect(response.json()).resolves.toEqual({ error: "backend failed" });
  });

  it("uses the upstream status as an empty error fallback when requested", async () => {
    const response = await relayJSON(new Response("", { status: 502 }), {
      errorFallback: true,
    });

    expect(response.status).toBe(502);
    await expect(response.json()).resolves.toEqual({ error: "Upstream 502" });
  });

  it("preserves an empty error without a fallback", async () => {
    const response = await relayJSON(new Response("", { status: 502 }));

    expect(response.status).toBe(502);
    await expect(response.json()).resolves.toEqual({ error: "" });
  });

  it("returns an empty object for a non-JSON OK body in lenient mode", async () => {
    const response = await relayJSON(new Response("not json"), {
      lenientBody: true,
    });

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({});
  });
});
