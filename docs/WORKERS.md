# Worker deployment

A worker needs a WorkerPort and an allowlist of handler implementations. It can use a scoped queue of an authority in the same process or page, the typed backplane client or the HTTP client. Keep the service's authenticated identity tied to a fixed worker ID, scope and handler allowlist.

## Remote workers

Mount the following handler on one POST route in your HTTPS application. Supply the application's credential validation and derive the principal on the server. This example takes a fixed scope, an existing scheduler and an authentication callback, so it can be used without an additional listener.

```ts
import { createWorkerEndpoint } from "defuss-scheduler/http";
import type { Scheduler, Scope } from "defuss-scheduler";

export function workerRoute(
  scheduler: Scheduler,
  verify: (request: Request) => Promise<{ workerId: string; scope: Scope; handlers: string[] } | null>,
) {
  return createWorkerEndpoint(async request => {
    const identity = await verify(request);
    if (!identity) return null;
    return { ...identity, port: scheduler.scope(identity.scope) };
  });
}
```

On the worker, pass the resulting endpoint URL and its scoped credential to connectWorker. Pass the returned port to startWorker. Worker IDs in claims and reports must match the principal. Capability claims must also be authorized. The HTTP endpoint accepts only worker operations and caps request bodies at 320000 bytes. Use HTTPS for remote connections. The client accepts plain HTTP on loopback for local deployments and rejects redirects.

Long-running handlers should cancel I/O when the context signal aborts and pass the stable run ID to external effects. The signal aborts when the lease is lost and when the run is terminated. A process that is stalled or disconnected can miss a cancellation. A storage system enforcing fencing must compare the decimal counter numerically, in the same scoped resource domain.

## Browser pages as workers

A page runs handlers in two ways. With an authority in the same page, pass its scoped queue to startWorker. As a remote worker of a Node.js or Bun authority, pass connectWorker's port. The client accepts HTTPS and loopback HTTP URLs. The endpoint answers only POST requests. A page on another origin therefore needs the application to answer the CORS preflight for the authorization header and the JSON content type. VERIFIED: `make e2e` runs a page as a remote worker against a route on the page's own origin in Chromium, Firefox and WebKit.

## Backplane

The defuss-backplane daemon opens one authority for every app, tenant and bucket under its data root. It serves twelve scheduler management and observation tools over MCP, including the schedule and run histories and the hard stop, and the whole queue over typed RPC. A schedule whose handler is `_rpc` needs no worker: the daemon asks `ready` for that handler and calls the defuss-rpc function that the schedule's payload names. Runs of other handlers go to workers such as this one.

```ts
import { connectBackplane } from "defuss-backplane/client";
import { startWorker } from "defuss-scheduler/worker";

const app = await connectBackplane({ app: "media" });
const queue = app.tenant("studio").scheduler("maintenance");
const worker = startWorker(queue, {
  workerId: "catalog-1",
  handlers: { refresh: async run => ({ processed: run.id }) },
});
process.once("SIGTERM", () => { void worker.stop(); });
```

The daemon binds to loopback and its bot key can select any local scope. For remote access, mount the authenticated worker route in the application and return a scoped backplane queue as the principal's port. The workers only need their own scoped credentials.
