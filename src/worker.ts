/** VERIFIED: The worker uses monotonic time to subtract RPC latency from lease life. Heartbeats keep a slow handler alive and stop aborts at its drain deadline. */
import type {
  WorkerPort,
  Run,
  Json,
  Grant,
  Outcome,
  ClaimResult,
} from "./types.js";
export type {
  WorkerPort,
  Run,
  Lease,
  Claim,
  ClaimResult,
  Outcome,
  Report,
  Json,
} from "./types.js";

export class PermanentFailure extends Error {}
export interface Context {
  signal: AbortSignal;
  runId: string;
  fence: string;
  attempt: number;
}
export type Handler = (
  run: Run,
  context: Context,
) => Json | void | Promise<Json | void>;
/** Parse and narrow untrusted payloads before invoking a typed handler. */
export function defineHandler<P, R extends Json | void>(
  parse: (payload: Json) => P,
  handle: (payload: P, ctx: Context) => R | Promise<R>,
): Handler {
  return (run, ctx) => {
    let value: P;
    try {
      value = parse(run.payload);
    } catch (error) {
      throw new PermanentFailure(
        error instanceof Error ? error.message : "Invalid payload",
      );
    }
    return handle(value, ctx);
  };
}
export interface WorkerOptions {
  workerId: string;
  handlers: Record<string, Handler>;
  concurrency?: number;
  capabilities?: string[];
  maxPayloadBytes?: number;
  onError?: (error: unknown) => void;
}
function delay(ms: number, signal: AbortSignal) {
  if (signal.aborted) return Promise.resolve();
  return new Promise<void>((resolve) => {
    function finish() {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    }
    const timer = setTimeout(finish, ms);
    signal.addEventListener("abort", finish, { once: true });
  });
}
/** Each local slot retries an ambiguous RPC with the original operation identity. */
export function startWorker(port: WorkerPort, options: WorkerOptions) {
  const concurrency = options.concurrency ?? 1;
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 256)
    throw new RangeError("concurrency must be 1..256");
  const handlers = new Map(Object.entries(options.handlers));
  if (
    !handlers.size ||
    [...handlers.values()].some((fn) => typeof fn !== "function")
  )
    throw new TypeError("Expected handlers");
  const validName = (s: unknown) =>
    typeof s === "string" &&
    !!s &&
    s.isWellFormed() &&
    new TextEncoder().encode(s).length <= 128;
  if (
    !validName(options.workerId) ||
    handlers.size > 64 ||
    ![...handlers.keys()].every(validName) ||
    (options.capabilities &&
      (options.capabilities.length > 64 ||
        !options.capabilities.every(validName))) ||
    (options.maxPayloadBytes !== undefined &&
      (!Number.isSafeInteger(options.maxPayloadBytes) ||
        options.maxPayloadBytes < 0))
  )
    throw new TypeError(
      "Invalid worker identity, capabilities or payload ceiling",
    );
  const stopped = new AbortController(),
    forced = new AbortController(),
    sessionId = crypto.randomUUID();
  const log = options.onError ?? console.error;
  async function execute(grant: Grant, receivedTtl: number) {
    const lost = new AbortController(),
      finished = new AbortController();
    const signal = AbortSignal.any([forced.signal, lost.signal]);
    let deadline = performance.now() + receivedTtl;
    function arm() {
      return setTimeout(
        () => lost.abort(),
        Math.max(0, deadline - performance.now()),
      );
    }
    let watchdog = arm();
    const heartbeat = (async () => {
      const stop = AbortSignal.any([signal, finished.signal]);
      while (!stop.aborted) {
        await delay(Math.max(1, (deadline - performance.now()) / 3), stop);
        if (stop.aborted) break;
        const request = { lease: grant.lease, requestId: crypto.randomUUID() };
        while (!stop.aborted) {
          const start = performance.now();
          try {
            const result = await port.renew(request);
            if (stop.aborted) break;
            if (
              !("ttlMs" in result) ||
              !result.ttlMs ||
              start + result.ttlMs <= performance.now()
            ) {
              lost.abort();
              break;
            }
            deadline = start + result.ttlMs;
            clearTimeout(watchdog);
            watchdog = arm();
            break;
          } catch (e) {
            log(e);
            await delay(
              Math.min(100, Math.max(1, (deadline - performance.now()) / 4)),
              stop,
            );
          }
        }
      }
    })();
    try {
      let send: () => Promise<Outcome>;
      const requestId = crypto.randomUUID();
      try {
        const result = await handlers.get(grant.run.handler)!(grant.run, {
          runId: grant.run.id,
          fence: grant.lease.fence,
          attempt: grant.run.attempt,
          signal,
        });
        send = () =>
          port.complete({
            lease: grant.lease,
            requestId,
            result: result ?? null,
          });
      } catch (e) {
        const error =
          (e instanceof Error ? e.message : String(e)).slice(0, 4096) ||
          "Handler failed";
        send = () =>
          port.fail({
            lease: grant.lease,
            requestId,
            error,
            permanent: e instanceof PermanentFailure,
          });
      }
      while (!signal.aborted) {
        try {
          await send();
          break;
        } catch (e) {
          log(e);
          const code = (e as { code?: string })?.code;
          if (code === "INVALID_ARGUMENT" || code === "REVISION_CONFLICT") {
            await port.fail({
              lease: grant.lease,
              requestId: crypto.randomUUID(),
              error: "Handler result rejected",
              permanent: true,
            });
            break;
          }
          await delay(100, signal);
        }
      }
    } finally {
      finished.abort();
      clearTimeout(watchdog);
      await heartbeat;
    }
  }
  async function slot(index: number) {
    let sequence = 0;
    while (!stopped.signal.aborted) {
      const request = {
        workerId: options.workerId,
        sessionId,
        slot: index,
        sequence: ++sequence,
        requestId: crypto.randomUUID(),
        handlers: [...handlers.keys()],
        capabilities: options.capabilities ?? [],
        ...(options.maxPayloadBytes !== undefined && {
          maxPayloadBytes: options.maxPayloadBytes,
        }),
      };
      while (!stopped.signal.aborted) {
        const start = performance.now();
        let reply: ClaimResult;
        try {
          reply = await port.claim(request);
        } catch (error) {
          log(error);
          await delay(100, stopped.signal);
          continue;
        }
        if (reply.kind === "granted" && !forced.signal.aborted) {
          const remaining = reply.ttlMs - (performance.now() - start);
          if (remaining > 0) {
            // Once the handler has started, a report transport error must never
            // send this slot back through claim replay for the same attempt.
            try {
              await execute(reply, remaining);
            } catch (error) {
              log(error);
            }
          } else await delay(10, stopped.signal);
        } else if (reply.kind !== "granted") {
          await delay(
            reply.retryAfterMs * (1 + Math.random() / 4),
            stopped.signal,
          );
        }
        break;
      }
    }
  }
  const done = Promise.all(
    Array.from({ length: concurrency }, (_, i) => slot(i)),
  ).then(() => {});
  return {
    done,
    async stop({ drainMs = 15_000 }: { drainMs?: number } = {}) {
      if (!Number.isFinite(drainMs) || drainMs < 0)
        throw new RangeError("Invalid drain deadline");
      stopped.abort();
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          done,
          new Promise<void>((resolve) => {
            timer = setTimeout(() => {
              forced.abort();
              resolve();
            }, drainMs);
          }),
        ]);
      } finally {
        clearTimeout(timer);
      }
    },
  };
}
