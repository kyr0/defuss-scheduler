/** VERIFIED: The endpoint binds each grant request to its authenticated worker and scoped port. The HTTP test exercises a real listener and rejected credentials. */
import { SchedulerError, type WorkerPort } from "./types.js";
export type { WorkerPort } from "./types.js";

export interface WorkerPrincipal {
  port: WorkerPort;
  workerId: string;
  handlers: readonly string[];
  capabilities?: readonly string[];
}
const methods = ["claim", "renew", "complete", "fail"] as const;
type Method = (typeof methods)[number];
const status = {
  INVALID_ARGUMENT: 400,
  ALREADY_EXISTS: 409,
  NOT_FOUND: 404,
  REVISION_CONFLICT: 409,
  BUSY: 503,
  CLOSED: 503,
  OWNER_BUSY: 409,
  FORMAT: 500,
  COMMIT_UNCERTAIN: 503,
} as const;
function reply(value: unknown, code = 200) {
  return Response.json(value, {
    status: code,
    headers: { "cache-control": "no-store" },
  });
}
async function body(request: Request) {
  const limit = 320000;
  if (Number(request.headers.get("content-length")) > limit) return null;
  const reader = request.body?.getReader();
  if (!reader) throw new TypeError("Missing request body");
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.length;
      if (length > limit) {
        await reader.cancel();
        return null;
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const data = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    data.set(chunk, offset);
    offset += chunk.length;
  }
  return {
    value: JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(data)),
  };
}
/** Mount at one application-owned path. Authorization binds a worker to its scoped port. */
export function createWorkerEndpoint(
  authorize: (
    request: Request,
  ) => WorkerPrincipal | null | Promise<WorkerPrincipal | null>,
) {
  return async (request: Request): Promise<Response> => {
    if (request.method !== "POST")
      return reply({ error: "POST required" }, 405);
    const principal = await authorize(request);
    if (!principal) return reply({ error: "Unauthenticated" }, 401);
    if (
      request.headers.get("content-type")?.split(";")[0].trim() !==
      "application/json"
    )
      return reply({ error: "JSON required" }, 415);
    try {
      const item = await body(request);
      if (!item) return reply({ error: "Body too large" }, 413);
      const { method, input } = item.value;
      if (!methods.includes(method) || !input || typeof input !== "object")
        return reply({ error: "Invalid request" }, 400);
      if (
        (method === "claim" ? input.workerId : input.lease?.workerId) !==
        principal.workerId
      )
        return reply({ error: "Forbidden worker" }, 403);
      if (
        method === "claim" &&
        (!Array.isArray(input.handlers) ||
          !input.handlers.every((h: unknown) =>
            principal.handlers.includes(h as string),
          ) ||
          !Array.isArray(input.capabilities ?? []) ||
          !(input.capabilities ?? []).every((c: unknown) =>
            principal.capabilities?.includes(c as string),
          ))
      )
        return reply({ error: "Forbidden handler or capability" }, 403);
      return reply(await principal.port[method as Method](input));
    } catch (e) {
      if (e instanceof SchedulerError)
        return reply({ error: e.message, code: e.code }, status[e.code]);
      if (e instanceof TypeError || e instanceof SyntaxError)
        return reply({ error: "Invalid worker request" }, 400);
      return reply({ error: "Scheduler request failed" }, 500);
    }
  };
}
export function connectWorker(
  url: string,
  options: { token: string; timeoutMs?: number },
): WorkerPort {
  const endpoint = new URL(url);
  if (
    endpoint.username ||
    endpoint.password ||
    (endpoint.protocol !== "https:" &&
      !(
        endpoint.protocol === "http:" &&
        ["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname)
      ))
  )
    throw new TypeError("Worker endpoint requires HTTPS or loopback HTTP");
  async function call<K extends Method>(
    method: K,
    input: Parameters<WorkerPort[K]>[0],
  ): Promise<Awaited<ReturnType<WorkerPort[K]>>> {
    const response = await fetch(endpoint, {
      method: "POST",
      redirect: "error",
      headers: {
        "content-type": "application/json",
        authorization: "Bearer " + options.token,
      },
      body: JSON.stringify({ method, input }),
      signal: AbortSignal.timeout(options.timeoutMs ?? 10000),
    });
    const value = await response.json();
    if (!response.ok) {
      if (Object.hasOwn(status, value?.code))
        throw new SchedulerError(value.code, value.error);
      throw new Error("Worker HTTP " + response.status);
    }
    return value;
  }
  return {
    claim: (i) => call("claim", i),
    renew: (i) => call("renew", i),
    complete: (i) => call("complete", i),
    fail: (i) => call("fail", i),
  };
}
