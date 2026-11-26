# defuss-scheduler

defuss-scheduler turns cron schedules into durable work, with one API in Node.js, Bun and the browser. One authority owns a store and records each due occurrence as a run. Workers pull runs from it. A worker is a set of handler functions in the same process or page, or a program on another machine. The authority grants one current lease per concurrency key. A worker that loses its lease cannot renew, fail or complete the next worker's attempt. Besides schedules, the authority takes one-off jobs, each with an optional deadline and a follow-up job for each way it can end.

| Runtime | Store | Authority |
| --- | --- | --- |
| Node.js, Bun | A directory on local disk | One process, which holds an operating-system file lock |
| Browser | A named store in the origin's private file system | One window or dedicated worker of the origin, which holds a Web Lock |

## Install and run

```sh
bun i defuss-scheduler
```

Save this as `demo.mjs` and run `bun demo.mjs` or `node demo.mjs`. It waits for a scheduled job, prints the occurrence, drains the worker and closes the scheduler.

```js
import { openScheduler, startWorker } from "defuss-scheduler";

const scheduler = await openScheduler({ directory: "./var/jobs" });
const jobs = scheduler.scope({ app: "media", tenant: "studio", bucket: "maintenance" });
if (!(await jobs.get("catalog"))) {
  await jobs.create({
    id: "catalog",
    cron: "*/2 * * * * *",
    handler: "refresh",
    payload: { catalog: "public" },
    concurrencyKey: "catalog-db",
  });
}
let finished;
const first = new Promise(resolve => { finished = resolve; });
const worker = startWorker(jobs, {
  workerId: "local-1",
  handlers: {
    refresh(run, ctx) {
      console.log(ctx.runId, new Date(run.scheduledAt).toISOString(), run.payload);
      finished();
      return { refreshed: true };
    },
  },
});
try {
  await first;
} finally {
  await worker.stop();
  await scheduler.close();
}
```

Every attempt of an occurrence has the same run ID. Pass it to the destination as an idempotency key when a handler changes external state. A paused or partitioned worker may still execute after its lease expires.

## How it fits together

| Term | Meaning |
| --- | --- |
| Authority | The one process, window or worker that has a store open. It turns due cron occurrences into runs and decides who may execute them. |
| Scope | An app, tenant and bucket. Schedule IDs, concurrency keys and worker groups are isolated per scope. `scheduler.scope(...)` returns the queue of one scope. |
| Schedule | A cron expression, a handler name and a JSON payload. It can be updated, paused, resumed, removed and terminated. |
| Run | One occurrence of a schedule, or one job. Every attempt of it carries the same run ID. |
| Job | Work to be done once, without a cron, under an ID of the caller's. A worker claims it like an occurrence. |
| Follow-up | A job the authority admits when a job ends, in the same transaction, for example to notify the application. |
| Worker | A loop that claims runs for the handler names it implements and reports the outcome. |
| Lease and fence | A claim grants a lease that the worker renews while it runs. Each grant in a concurrency group carries a greater fence, so a destination can reject a writer whose lease is gone. |

## Where the data lives

The scheduler stores a decision before it acts on it. Schedules, runs, leases, fences and request receipts are documents in an embedded [defuss-db](https://github.com/kyr0/defuss-db) 3.2.0 database named `defuss_scheduler`. The engine ships inside this package, so there is no database server to run.

You choose the location with the required `directory` option. Under Node.js and Bun it is any path on local disk. The scheduler creates it with mode 0700 if it is missing. In a browser it names a store in the origin's private file system.

```js
const scheduler = await openScheduler({
  directory: process.env.SCHEDULER_DIR ?? "./var/jobs",
});
```

| Collection | Content | History and timestamps |
| --- | --- | --- |
| schedules | One document per schedule: its definition, state and revision | Tracked |
| runs | One document per occurrence: status, attempt, error and result | Tracked |
| ledger | Calendar cursors, queue and expiry indexes, leases, fences, worker slots and request receipts | Not tracked |

One authority owns a directory at a time. A second `openScheduler` on it rejects with `OWNER_BUSY`. To back a store up, stop its authority and copy the whole directory.

Retention removes a terminal run together with its history, so the store holds the unfinished runs and the runs of the last `retentionMs`. VERIFIED: A probe on 2026-11-23 under Node.js 26.13.1 completed 300 runs with a small payload. The compacted store held 3,531 KiB before retention and 29.5 KiB after it, with two unfinished runs left. A schedule's history is kept and grows by one version per edit. The database compacts its log by itself.

## Schedules

```js
const report = await jobs.create({
  id: "nightly-report",
  ref: "report",
  cron: "30 2 * * *",
  timezone: "Europe/Berlin",
  handler: "report",
  payload: { format: "pdf" },
  misfire: "latest",
  retry: { maxAttempts: 5, baseMs: 1000 },
});

const edited = await jobs.update(
  "nightly-report",
  { payload: { format: "html" } },
  { revision: report.revision, requestId: crypto.randomUUID() },
);
const paused = await jobs.pause("nightly-report", {
  revision: edited.revision,
  requestId: crypto.randomUUID(),
});
```

| Field | Default | Meaning |
| --- | --- | --- |
| id | required | 1 to 128 UTF-8 bytes, unique in its scope. A removed or terminated schedule keeps its ID reserved. |
| ref | none | A name of your own, 1 to 128 UTF-8 bytes, fixed at creation. It may repeat across schedules. |
| cron | required | Five fields from minute to weekday, or six with seconds first. Lists, ranges and steps are supported. |
| handler | required | The name a worker registers a function under. |
| payload | null | JSON of up to 128 KiB, handed to the handler. |
| timezone | UTC | An IANA zone name for local-time schedules. |
| misfire | latest | What happens to occurrences the authority missed: `latest` runs the most recent one, `catchUp` runs each of them, `skip` drops those older than `graceMs`. |
| graceMs | 60000 | The window `skip` still accepts. |
| retry | 3 attempts, 500 ms base, 30 s cap | `maxAttempts`, `baseMs` and `maxMs` of the jittered exponential backoff. |
| capabilities | none | Names a worker must offer to receive the run. |
| concurrencyKey, parallel | one run per schedule at a time | See the table below. |

`get` and `list` read schedules. `update`, `pause`, `resume` and `remove` take the schedule's current `revision` and a `requestId`. A stale revision rejects with `REVISION_CONFLICT`. After an unclear response, repeat the call with the same request ID and arguments to get the original result. `nextOccurrence(cron, timezone, after)` computes the next matching instant without a store.

### Name work with a ref

A schedule ID is used once per scope. A `ref` is a name that outlives one schedule: the schedule that replaces a removed one carries the same ref under a new ID. Each run copies the ref of its schedule when it is admitted.

```js
await jobs.create({ id: "report-v2", ref: "report", cron: "0 3 * * *", handler: "report" });

const { items: schedules } = await jobs.list({ ref: "report" });
const { items: runs } = await jobs.listRuns({ ref: "report", limit: 20 });
```

`list({ ref })` returns every schedule that carries the ref, removed and terminated ones included. `listRuns({ ref })` returns their runs in scheduled order, across all of those schedules.

### Choose the concurrency domain

| Requirement | Definition |
| --- | --- |
| Serialize occurrences of one schedule | Omit concurrencyKey and parallel. The next occurrence waits while the prior attempt has a live lease. |
| Serialize different schedules that change one resource | Give them the same concurrencyKey in the same app, tenant and bucket. |
| Run occurrences independently | Set parallel to true. The worker's concurrency option controls its local slot count. |

The default latest policy coalesces ticks missed by the authority. An already materialized occurrence waits in the queue while its group is busy. Choose catchUp for every missed occurrence or skip with graceMs for a bounded time window.

## Jobs

```js
const job = await jobs.submit({
  id: "render-42",
  handler: "render",
  payload: { page: 42 },
  timeoutMs: 600_000,
  on: {
    succeeded: { handler: "notify", payload: { page: 42 } },
    dead: { handler: "alert" },
    timedOut: { handler: "alert" },
  },
});
const record = await jobs.getJob("render-42"); // state, attempt, lastError and, once it succeeded, result
```

| Field | Default | Meaning |
| --- | --- | --- |
| id | required | 1 to 128 UTF-8 bytes, unique in the scope while the job's record exists. |
| handler, payload, capabilities, retry | as for schedules | The same rules as a schedule's fields. |
| ref | none | A name of your own. `listRuns({ ref })` and `terminate({ ref })` reach the jobs that carry it. |
| availableAt | now | Epoch milliseconds before which no worker gets the job. |
| timeoutMs | none | A deadline counted from submission, 1 ms to a year. Past it, the job ends as `timedOut`, wherever it stands. |
| concurrencyKey | none | Jobs run in parallel unless they share a key, with each other or with schedules. |
| on | none | A follow-up for any of the endings `succeeded`, `dead`, `timedOut` and `terminated`: a `handler`, and optionally a `payload`, `capabilities` and `retry`. |

`submit` returns the job's run, with `jobId` set and `scheduleId` null. A repeated call with the same `requestId` and arguments returns the job as first admitted; another ID for the same job rejects with `ALREADY_EXISTS`. A scope that holds `maxPending` unfinished runs rejects a submission with `BUSY`. Retention removes a finished job one day after it ends by default, and then its ID is free again. Why not reserve job IDs forever, as schedule IDs are: a queue of many jobs would keep one record per job ever submitted.

A deadline that passes ends a waiting job without a claim. A leased job ends too: the holder's next renewal answers `stale`, so the handler's signal aborts, and a completion after the deadline is ignored. VERIFIED by tests/jobs.test.ts.

A follow-up enters the queue in the transaction that ends its job, so a crash leaves both or neither. It is a run with `cause: { job, run, state }`, which names the job and how it ended. A follow-up carries the job's ref and has no `on` of its own, so follow-ups cannot loop. Why the authority admits them rather than the application watching for endings: lease expiry, retries running out and deadlines happen inside the authority's maintenance, where no call of the application passes. A follow-up counts toward `maxPending`, but it is admitted even when the scope is full, so an ending never fails. A hard stop by job, ref or scope never ends a follow-up, so the application learns of every ending; `terminate({ run })` ends one.

## Runs and their status over time

A run moves through `ready`, `leased`, `retryWait` and ends as `succeeded`, `dead`, `terminated`, or for a job past its deadline `timedOut`. `getRun` and `listRuns` return the current state with the attempt count, the last error, the result and two timestamps. `createdAt` is the instant the occurrence was admitted. `updatedAt` is its last status change. Schedules carry the same pair for their definition and state. Terminal runs are removed with their history after `retentionMs`, one day by default.

The history answers what happened earlier. `runHistory` lists every status a run went through, newest first, and is empty once retention removed the run. `scheduleHistory` lists every definition and state of a schedule.

```js
const { items } = await jobs.listRuns({ limit: 20 });
for (const version of await jobs.runHistory(items[0].id)) {
  console.log(new Date(version.from).toISOString(), version.value?.state);
}
// 2026-11-22T22:03:40.708Z succeeded
// 2026-11-22T22:03:40.703Z leased
// 2026-11-22T22:03:39.735Z ready
```

Each version holds `from`, `until` and `value`. `until` is null for the latest version. `value` is null only for a removed run whose history is not erased yet. A lease renewal is not a status change and adds no version. The calendar position of a schedule moves with every occurrence and is not part of its history.

## Stop work at once

```js
await jobs.terminate({ run: runId }, { cause: "wrong input file" });
await jobs.terminate({ schedule: "nightly-report" }, { cause: "replaced by report-v2" });
await jobs.terminate({ ref: "report" }, { cause: "customer offboarded" });
const { schedules, runs } = await jobs.terminate({ all: true }, { cause: "maintenance" });
```

| Target | Terminates |
| --- | --- |
| `{ run: id }` | One unfinished run, a follow-up included. |
| `{ job: id }` | One unfinished job. |
| `{ schedule: id }` | The schedule and its unfinished runs. |
| `{ ref }` | Every schedule and job that carries the ref, and the schedules' unfinished runs. |
| `{ all: true }` | Every schedule, unfinished run and job of the scope. Follow-ups stay. |

`pause`, `remove` and `update` wait until the occurrences that were already due have been admitted, and they reject with `BUSY` until then. `terminate` does not wait. An active or paused schedule becomes `terminated`: it admits nothing more and its ID stays reserved. Every unfinished run ends as `terminated`, whether it was ready, waiting for a retry or leased. A removed schedule keeps its state, and its unfinished runs end all the same.

Schedules and runs keep the cause in `terminationCause`, and their histories show the change. The call returns how many schedules and runs it terminated, so a repeated call returns zeros. A handler that is executing a terminated run sees its `signal` abort when the worker next renews the lease, and its report is ignored.

## Workers

```js
import { startWorker, defineHandler, PermanentFailure } from "defuss-scheduler";

const worker = startWorker(jobs, {
  workerId: "reports-1",
  concurrency: 4,
  handlers: {
    report: defineHandler(
      (payload) => {
        if (payload?.format !== "pdf" && payload?.format !== "html")
          throw new Error("unknown format");
        return payload;
      },
      async (payload, ctx) => {
        await render(payload.format, { idempotencyKey: ctx.runId, signal: ctx.signal });
        return { rendered: payload.format };
      },
    ),
  },
});
process.once("SIGTERM", () => worker.stop());
```

`startWorker` takes a queue or any other worker port, a `workerId` and the handler functions by name. `concurrency` sets how many runs it executes at once, from 1 to 256. A handler receives the run and a context with `runId`, `attempt`, `fence` and an abort `signal`. Its return value becomes the run's JSON result. The example's `render` stands for the application's own function.

A thrown error fails the attempt, and the run is retried within its retry budget. Throwing `PermanentFailure` ends the run as `dead` at once. `defineHandler` validates the payload first, and a rejected payload counts as a permanent failure. The worker renews its lease while a handler runs. When the lease is lost or the run is terminated, the context's signal aborts. A slot without work waits `pollMs`, plus up to a quarter of it as jitter, before it asks again. An empty answer writes nothing to the store. `stop` ends claiming and waits up to `drainMs`, 15 seconds by default, for handlers in flight.

A worker that serves every scope, such as a daemon that runs one handler for all of its tenants, asks the authority `ready({ handlers })` for the scopes that hold a run of those handlers that is available now, and claims only there. An idle scope then costs no claims. `ready` writes nothing to the store.

`poolHandlers(pool, names)` runs each named handler as a task of a worker-thread pool, such as defuss-multicore's `createPool`, off the event loop that renews the lease. The handler's signal cancels the task, which with defuss-multicore terminates its thread. The pool is a structural type, so this package does not depend on any pool library. [Worker threads](docs/WORKERS.md#worker-threads)

Workers on other machines reach the authority through one HTTP route that the application mounts behind its own authentication. [Remote worker and backplane integration](docs/WORKERS.md)

## Authority options

| Option | Default | Meaning |
| --- | --- | --- |
| directory | required | The store location, as described above. |
| leaseMs | 30000 | How long a grant lasts without renewal. |
| tickMs | 250 | Pause between maintenance turns, which admit due occurrences, expire leases and collect what retention removes. A retention backlog is collected without the pause. |
| pollMs | 250 | Delay the authority suggests to a worker after an empty claim. |
| retentionMs | 86400000 | How long terminal runs and request receipts stay readable. |
| maxPending | 100000 | Upper bound of unfinished runs per scope. |
| autoTick | true | Set false to drive maintenance with `tick()` yourself. |

`close()` drains admitted commands and releases the store. The complete reference, including the pull protocol and every error code, is in the [API reference](docs/API.md).

## Run it in a browser

A page can be the authority and run the handler functions itself, with no server involved. The imports stay the same: a bundler that honors the `browser` export condition selects a build without Node.js imports. In a browser, `directory` names the store with 1 to 64 characters from a-z, 0-9, `_` and `-`.

```js
import { openScheduler, startWorker } from "defuss-scheduler";

const scheduler = await openScheduler({ directory: "jobs" });
const jobs = scheduler.scope({ app: "notes", tenant: "local", bucket: "sync" });
if (!(await jobs.get("autosave"))) {
  await jobs.create({ id: "autosave", cron: "*/30 * * * * *", handler: "save" });
}
startWorker(jobs, {
  workerId: "page",
  handlers: {
    save(run) {
      localStorage.setItem("saved", new Date(run.scheduledAt).toISOString());
      return { saved: true };
    },
  },
});
```

Schedules and runs persist when the page closes and when the browser restarts. Occurrences that came due while no page was open follow the schedule's misfire policy at the next open. One window or dedicated worker per origin owns a store. While it does, `openScheduler` with the same name rejects with `OWNER_BUSY` in every other window. Retry it to take over once the owner has closed the scheduler or its page is gone. A lease the previous owner held expires, and the run is handed out again with a greater fence.

The browser build needs Web Locks and an origin private file system with writable file streams. Browsers expose both in secure contexts, which means HTTPS or localhost. A page can also be a remote worker of a Node.js or Bun authority: `connectWorker` from `defuss-scheduler/http` uses `fetch`.

VERIFIED: `make e2e` bundles the packed package with Bun and runs it in Chromium 159, Firefox 159 and WebKit 27.2 on macOS arm64, driven by Playwright 1.65.1. The run covers a window, a dedicated worker, a second window, the loss of the owning page, a browser restart and the erasure of a run's history after retention. No test run has exercised hidden tabs, private windows, service workers, shared workers or mobile browsers.

## Entry points

| Import | Surface |
| --- | --- |
| defuss-scheduler | openScheduler, nextOccurrence, startWorker, defineHandler, PermanentFailure, SchedulerError and public types. The `browser` export condition selects the build for pages and dedicated workers. Every other resolution gets the Node.js and Bun build. |
| defuss-scheduler/worker | Worker loop, typed handler registration and worker protocol types. This entry loads without any storage or locking module in every runtime. |
| defuss-scheduler/http | Fetch-based worker client for Node.js, Bun and pages, and a handler that an application mounts behind its own authentication. |

[API reference](docs/API.md) · [Crash recovery and store operations](ARCH.md) · [Remote worker and backplane integration](docs/WORKERS.md)

## Build and verify

The source targets Node.js 26 and Bun 1.4.5 or newer. The verified versions are pinned in mise.toml. Run `make setup`, then `make verify`. Setup also downloads the Chromium, Firefox and WebKit builds of the pinned Playwright version. `make e2e` installs the packed artifact in a clean consumer and restarts real processes across Node.js and Bun. It then bundles the browser consumer and runs it in the three engines. `make lint` type-checks the browser entries without Node.js typings, so a Node.js global in shared code fails the build. `make bench` records local measurements.
