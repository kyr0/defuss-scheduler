/** VERIFIED: The bundled page runs the installed browser build in a window, in a dedicated worker and as a remote worker of a native authority. */
import {
  openScheduler,
  startWorker,
  defineHandler,
  nextOccurrence,
  PermanentFailure,
  type Grant,
  type Queue,
  type Run,
} from "defuss-scheduler";
import { connectWorker } from "defuss-scheduler/http";

const scope = { app: "page", tenant: "origin", bucket: "maintenance" };
const fast = { leaseMs: 1500, tickMs: 25, pollMs: 15 };
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const code = (error: unknown) => (error as { code?: string })?.code;
function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
async function until<T>(
  read: () => Promise<T | null | undefined | false>,
  what: string,
): Promise<T> {
  const end = Date.now() + 10000;
  for (;;) {
    const value = await read();
    if (value) return value;
    check(Date.now() < end, "Timed out waiting for " + what);
    await sleep(20);
  }
}
/** OWNER_BUSY lasts until the previous owner's lock is gone, BUSY until an accepted edit has drained. */
function retry<T extends object>(busy: string, run: () => Promise<T>) {
  return until(
    () =>
      run().catch((error) => {
        if (code(error) !== busy) throw error;
        return null;
      }),
    "the end of " + busy,
  );
}
async function rejects(expected: string, attempt: Promise<unknown>) {
  const actual = await attempt.then(
    () => "no error",
    (error) => code(error) ?? String(error),
  );
  check(actual === expected, "Expected " + expected + ", got " + actual);
}
const findRun = (q: Queue, match: (run: Run) => boolean) =>
  until(
    async () => (await q.listRuns({ limit: 500 })).items.find(match),
    "a matching run",
  );
function claimer(q: Queue, sessionId: string) {
  let sequence = 0;
  return () =>
    until(async () => {
      const reply = await q.claim({
        workerId: "manual",
        sessionId,
        slot: 0,
        sequence: ++sequence,
        requestId: "c" + sequence,
        handlers: ["hold"],
      });
      return reply.kind === "granted" && reply;
    }, "a grant");
}
const storedGrant = (): Grant => JSON.parse(localStorage.getItem("grant")!);

/** Cron, worker functions and schedule management in one page. It leaves a lease held and never closes. */
async function first() {
  const gap = nextOccurrence(
    "30 2 * * *",
    "Europe/Berlin",
    Date.UTC(2026, 2, 28, 23),
  );
  check(gap === Date.UTC(2026, 2, 29, 1), "Berlin spring gap");
  await rejects("INVALID_ARGUMENT", openScheduler({ directory: "Not/a/name" }));
  const owner = await openScheduler({ directory: "e2e", ...fast });
  const q = owner.scope(scope);
  const tick = await q.create({
    id: "tick",
    cron: "* * * * * *",
    handler: "double",
    payload: 21,
  });
  const bad = await q.create({
    id: "bad",
    cron: "* * * * * *",
    handler: "reject",
  });
  const worker = startWorker(q, {
    workerId: "page",
    concurrency: 2,
    handlers: {
      double: defineHandler(
        (value) => {
          if (typeof value !== "number") throw Error("number expected");
          return value;
        },
        (value, ctx) => ({ doubled: value * 2, fence: ctx.fence }),
      ),
      reject: () => {
        throw new PermanentFailure("refused");
      },
    },
  });
  const done = await findRun(q, (r) => r.state === "succeeded");
  check((done.result as { doubled: number }).doubled === 42, "handler result");
  const dead = await findRun(q, (r) => r.state === "dead");
  check(dead.lastError === "refused" && dead.attempt === 1, "permanent fault");

  const paused = await retry("BUSY", () =>
    q.pause("bad", { revision: bad.revision, requestId: "pause" }),
  );
  check(paused.state === "paused" && paused.nextDue === null, "pause");
  const resumed = await q.resume("bad", {
    revision: paused.revision,
    requestId: "resume",
  });
  check(resumed.state === "active" && resumed.nextDue !== null, "resume");
  const removed = await retry("BUSY", () =>
    q.remove("bad", { revision: resumed.revision, requestId: "remove" }),
  );
  check(removed.state === "removed", "remove");
  const edited = await retry("BUSY", () =>
    q.update(
      "tick",
      { payload: 5 },
      { revision: tick.revision, requestId: "edit" },
    ),
  );
  check((await q.get("tick"))?.revision === edited.revision, "get");
  check((await q.list()).items.length === 2, "list");
  const later = await findRun(
    q,
    (r) => r.state === "succeeded" && r.scheduleRevision === edited.revision,
  );
  check((later.result as { doubled: number }).doubled === 10, "edited payload");
  const page = await q.listRuns({ limit: 1 });
  check(page.cursor, "run cursor");
  const next = await q.listRuns({ limit: 1, cursor: page.cursor });
  check(next.items[0].id !== page.items[0].id, "second run page");
  await retry("BUSY", () =>
    q.pause("tick", { revision: edited.revision, requestId: "quiet" }),
  );
  await worker.stop();

  const held = await q.create({
    id: "held",
    cron: "* * * * * *",
    handler: "hold",
    retry: { baseMs: 0 },
  });
  const grant = await claimer(q, "first")();
  await retry("BUSY", () =>
    q.pause("held", { revision: held.revision, requestId: "hold" }),
  );
  const renewal = await q.renew({ lease: grant.lease, requestId: "renew" });
  check(renewal.kind === "applied", "renew");
  await owner.tick();
  await owner.compact();
  check((await q.getRun(grant.run.id))?.state === "leased", "lease persists");
  localStorage.setItem("grant", JSON.stringify(grant));
  return { succeeded: done.id, dead: dead.id, fence: grant.lease.fence };
}

/** A second page of the same origin while the first one is the authority. */
async function busy() {
  await rejects("OWNER_BUSY", openScheduler({ directory: "e2e" }));
  return { blocked: true };
}

/** After the owning page died: take over its store and settle the lease it held. */
async function reopen() {
  const owner = await retry("OWNER_BUSY", () =>
    openScheduler({ directory: "e2e", ...fast }),
  );
  const q = owner.scope(scope);
  const prior = storedGrant();
  const claim = claimer(q, "reopen");
  let grant = await claim();
  // An occurrence admitted before the pause shares the group and may come first.
  while (grant.run.id !== prior.run.id) {
    await q.complete({ lease: grant.lease, requestId: "extra" });
    grant = await claim();
  }
  check(grant.run.attempt === 2, "second attempt");
  check(grant.lease.token !== prior.lease.token, "fresh token");
  check(BigInt(grant.lease.fence) > BigInt(prior.lease.fence), "greater fence");
  const stale = await q.complete({ lease: prior.lease, requestId: "stale" });
  check(stale.kind === "stale", "stale completion");
  const late = await q.fail({
    lease: prior.lease,
    requestId: "late",
    error: "late",
  });
  check(late.kind === "stale", "stale failure");
  const input = { lease: grant.lease, requestId: "done", result: { rows: 1 } };
  check((await q.complete(input)).kind === "applied", "completion");
  check((await q.complete(input)).kind === "alreadyApplied", "replay");
  await owner.close();
  await rejects("CLOSED", q.get("tick"));
  // An awaited close has released the store, so the next open needs no retry.
  await (await openScheduler({ directory: "e2e" })).close();
  return { fence: grant.lease.fence, attempts: grant.run.attempt };
}

/** After the browser itself restarted on the same profile. */
async function restart() {
  const owner = await openScheduler({ directory: "e2e", ...fast });
  try {
    const q = owner.scope(scope);
    const run = await q.getRun(storedGrant().run.id);
    check(run?.state === "succeeded" && run.attempt === 2, "run survived");
    check((run.result as { rows: number }).rows === 1, "result survived");
    const tick = await q.get("tick");
    check(tick?.payload === 5 && tick.state === "paused", "schedule survived");
    check((await q.get("bad"))?.state === "removed", "tombstone survived");
    check(run.createdAt < run.updatedAt, "run timestamps");
    const statuses = (await q.runHistory(run.id)).map((v) => v.value?.state);
    check(
      statuses.join() === "succeeded,leased,retryWait,leased,ready",
      "run history " + statuses,
    );
    const edits = await q.scheduleHistory("tick");
    check(edits.length === 3 && edits[2].value?.payload === 21, "edit history");
    check(edits[0].value?.state === "paused", "latest schedule version");

    // A ref finds a schedule's runs without their IDs, and a hard stop by ref ends them where they stand.
    await q.create({ id: "ops-1", ref: "ops", cron: "* * * * * *", handler: "hold" });
    const admitted = await findRun(q, (r) => r.ref === "ops");
    const stop = await q.terminate({ ref: "ops" }, { cause: "restart check" });
    check(stop.schedules === 1 && stop.runs >= 1, "hard stop by ref");
    const ended = await q.getRun(admitted.id);
    check(ended?.state === "terminated", "terminated status");
    check(ended.terminationCause === "restart check", "termination cause");
    const named = await q.list({ ref: "ops" });
    check(named.items.length === 1, "schedules by ref");
    check(named.items[0].state === "terminated", "terminated schedule");
    const listed = (await q.listRuns({ ref: "ops" })).items;
    check(listed.some((r) => r.id === admitted.id), "runs by ref");
    return {
      state: run.state,
      attempts: run.attempt,
      statuses,
      terminated: ended.state,
    };
  } finally {
    await owner.close();
  }
}

/** The page pulls work from a native authority through the route its own server mounts. */
async function remote() {
  let started!: (fence: string) => void;
  const ran = new Promise<string>((resolve) => (started = resolve));
  const worker = startWorker(
    connectWorker(location.origin + "/worker", { token: "integration" }),
    {
      workerId: "machine",
      handlers: {
        rollup: (_run, ctx) => {
          started(ctx.fence);
          return { from: "browser" };
        },
      },
    },
  );
  const fence = await ran;
  await worker.stop();
  return { fence };
}

/** Authority and worker function inside a dedicated worker, which has no window. */
async function thread() {
  const owner = await openScheduler({
    directory: "e2e-thread",
    ...fast,
    retentionMs: 1000,
  });
  try {
    const q = owner.scope(scope);
    await q.create({ id: "beat", cron: "* * * * * *", handler: "beat" });
    let started!: (id: string) => void;
    const ran = new Promise<string>((resolve) => (started = resolve));
    const worker = startWorker(q, {
      workerId: "thread",
      handlers: {
        beat: (run) => {
          started(run.id);
          return "thread";
        },
      },
    });
    const id = await ran;
    await worker.stop();
    const run = await q.getRun(id);
    check(run?.state === "succeeded" && run.result === "thread", "thread run");
    // Retention erases the run with its history in the origin's storage as on disk.
    await until(async () => (await q.getRun(id)) === null, "retention");
    await until(
      async () => (await q.runHistory(id)).length === 0,
      "the erasure of the run's history",
    );
    return { context: "dedicated worker", state: run.state, erased: true };
  } finally {
    await owner.close();
  }
}
function dedicated() {
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL("./consumer-browser.js", location.href), {
      type: "module",
    });
    worker.onerror = (event) => reject(new Error(event.message));
    worker.onmessage = ({ data }) => {
      worker.terminate();
      if (data.error) reject(new Error(data.error));
      else resolve(data);
    };
  });
}

if (typeof document === "undefined")
  thread().then(
    (result) => postMessage(result),
    (error) => postMessage({ error: String(error) }),
  );
else
  Object.assign(window, {
    scenarios: { first, busy, reopen, restart, remote, dedicated },
  });
