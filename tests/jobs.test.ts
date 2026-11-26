/** VERIFIED: One-off jobs against real files: submission and replay, claims and results, deadlines, follow-ups by
 * ending across a restart, hard stops by job, ref and scope, and a real defuss-multicore pool behind a real worker. */
import { test, expect } from "bun:test";
import { strict as assert } from "node:assert";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createPool } from "defuss-multicore";
import {
  openScheduler,
  poolHandlers,
  startWorker,
  type Claim,
  type Grant,
  type Run,
  type SchedulerOptions,
} from "../src/index.js";
import type { tasks } from "./fixtures/pool-worker.ts";

const scope = { app: "studio", tenant: "acme", bucket: "jobs" };
const claim = (handler: string, sequence: number, slot = 0): Claim => ({
  workerId: "w",
  sessionId: "s",
  slot,
  sequence,
  requestId: "c" + sequence,
  handlers: [handler],
});
function grant(value: unknown): Grant {
  assert.equal((value as Grant).kind, "granted", JSON.stringify(value));
  return value as Grant;
}
async function fixture(opts: Partial<SchedulerOptions> = {}) {
  let now = Date.UTC(2026, 0, 1);
  const directory = await mkdtemp(join(tmpdir(), "defuss-scheduler-jobs-"));
  const options = { directory, autoTick: false, clock: () => now, leaseMs: 1000, retentionMs: 60_000, ...opts };
  let owner = await openScheduler(options);
  return {
    q: owner.scope(scope),
    get owner() {
      return owner;
    },
    advance: (ms: number) => void (now += ms),
    now: () => now,
    async restart() {
      await owner.close();
      owner = await openScheduler(options);
      this.q = owner.scope(scope);
    },
    async close() {
      await owner.close();
      await rm(directory, { recursive: true, force: true });
    },
  };
}
async function until(check: () => Promise<boolean>, ms = 10_000) {
  const end = Date.now() + ms;
  while (!(await check())) {
    if (Date.now() > end) throw new Error("timed out waiting");
    await Bun.sleep(10);
  }
}
const followUps = async (q: { listRuns(o?: object): Promise<{ items: Run[] }> }) =>
  (await q.listRuns({ limit: 500 })).items.filter((r) => r.cause !== null);

test("a job runs once without a cron: replay, a duplicate ID, a delayed start, the result, retries and the limits", async () => {
  const f = await fixture({ maxPending: 4 });
  try {
    const job = { id: "render-1", ref: "renders", handler: "render", payload: { page: 1 } };
    const admitted = await f.q.submit(job, { requestId: "r1" });
    expect(admitted).toMatchObject({ jobId: "render-1", scheduleId: null, scheduleRevision: null, ref: "renders", state: "ready", attempt: 0, deadline: null, on: null, cause: null });
    expect(await f.q.submit(job, { requestId: "r1" })).toEqual(admitted);
    await expect(f.q.submit(job, { requestId: "r2" })).rejects.toMatchObject({ code: "ALREADY_EXISTS" });
    await expect(f.q.submit({ ...job, id: "other" }, { requestId: "r1" })).rejects.toMatchObject({ code: "REVISION_CONFLICT" });
    const g = grant(await f.q.claim(claim("render", 1)));
    expect([g.run.id, g.run.jobId, g.run.payload]).toEqual([admitted.id, "render-1", { page: 1 }]);
    expect(await f.q.complete({ lease: g.lease, requestId: "done", result: { pages: 1 } })).toMatchObject({ kind: "applied" });
    expect(await f.q.getJob("render-1")).toMatchObject({ state: "succeeded", result: { pages: 1 }, attempt: 1 });
    expect(await f.q.getJob("absent")).toBeNull();
    expect((await f.q.listRuns({ ref: "renders" })).items.map((r) => r.id)).toEqual([admitted.id]);
    // A start in the future: nothing to claim until then.
    await f.q.submit({ id: "later", handler: "render", availableAt: f.now() + 1000 });
    expect((await f.q.claim(claim("render", 2))).kind).toBe("empty");
    f.advance(1000);
    const later = grant(await f.q.claim(claim("render", 3)));
    expect(later.run.jobId).toBe("later");
    await f.q.complete({ lease: later.lease, requestId: "later" });
    // Failures retry within the budget, then the job is dead with the last error.
    await f.q.submit({ id: "flaky", handler: "flaky", retry: { maxAttempts: 2, baseMs: 0 } });
    for (const sequence of [4, 5]) {
      const attempt = grant(await f.q.claim(claim("flaky", sequence)));
      await f.q.fail({ lease: attempt.lease, requestId: "f" + sequence, error: "broke " + sequence });
    }
    expect(await f.q.getJob("flaky")).toMatchObject({ state: "dead", attempt: 2, lastError: "broke 5" });
    await expect(f.q.submit({ id: "big", handler: "render", payload: "x".repeat(140_000) })).rejects.toMatchObject({ code: "INVALID_ARGUMENT" });
    await expect(f.q.submit({ id: "bad", handler: "render", timeoutMs: 0 })).rejects.toMatchObject({ code: "INVALID_ARGUMENT" });
    await expect(f.q.submit({ id: "odd", handler: "render", cron: "* * * * *" } as never)).rejects.toMatchObject({ code: "INVALID_ARGUMENT" });
    for (const n of [1, 2, 3, 4]) await f.q.submit({ id: "fill-" + n, handler: "idle" });
    await expect(f.q.submit({ id: "fill-5", handler: "idle" })).rejects.toMatchObject({ code: "BUSY" });
  } finally {
    await f.close();
  }
});

test("a deadline ends a job as timedOut wherever it stands, and a late report is stale", async () => {
  const f = await fixture();
  try {
    // Leased: the deadline passes, the renewal is stale, and so is a completion after it.
    await f.q.submit({ id: "slow", handler: "work", timeoutMs: 500 });
    const g = grant(await f.q.claim(claim("work", 1)));
    expect(g.run.deadline).toBe(f.now() + 500);
    f.advance(499);
    expect((await f.q.renew({ lease: g.lease, requestId: "r1" })).kind).toBe("applied");
    f.advance(1);
    expect((await f.q.renew({ lease: g.lease, requestId: "r2" })).kind).toBe("stale");
    expect((await f.q.complete({ lease: g.lease, requestId: "late", result: 1 })).kind).toBe("stale");
    expect(await f.q.getJob("slow")).toMatchObject({ state: "timedOut", result: null, finishedAt: f.now() });
    // Waiting for its start: maintenance ends it without a claim.
    await f.q.submit({ id: "waiting", handler: "work", timeoutMs: 100, availableAt: f.now() + 1000 });
    f.advance(100);
    await f.owner.tick();
    expect(await f.q.getJob("waiting")).toMatchObject({ state: "timedOut", attempt: 0 });
    // Ready but overdue before maintenance came to it: a claim ends it instead of granting it.
    await f.q.submit({ id: "queued", handler: "work", timeoutMs: 100 });
    f.advance(100);
    expect((await f.q.claim(claim("work", 2))).kind).toBe("empty");
    expect(await f.q.getJob("queued")).toMatchObject({ state: "timedOut", attempt: 0 });
  } finally {
    await f.close();
  }
});

test("a worker's handler sees its signal abort when the job's deadline passes, and its late result is dropped", async () => {
  const directory = await mkdtemp(join(tmpdir(), "defuss-scheduler-jobs-"));
  const owner = await openScheduler({ directory, tickMs: 10, leaseMs: 300 });
  const q = owner.scope(scope);
  let aborted = false;
  const worker = startWorker(q, {
    workerId: "w",
    handlers: {
      stuck: (_run, ctx) =>
        new Promise((done) =>
          ctx.signal.addEventListener("abort", () => {
            aborted = true;
            done({ late: true });
          }),
        ),
    },
  });
  try {
    await q.submit({ id: "stuck", handler: "stuck", timeoutMs: 400 });
    await until(async () => aborted && (await q.getJob("stuck"))?.state === "timedOut");
    await worker.stop();
    expect((await q.getJob("stuck"))?.result).toBeNull();
  } finally {
    await worker.stop();
    await owner.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("each ending admits its follow-up with the cause, it survives a restart, and only a job's own ID stops it", async () => {
  const f = await fixture();
  const note = (kind: string) => ({ handler: "notify", payload: { kind } });
  const on = { succeeded: note("ok"), dead: note("dead"), timedOut: note("late"), terminated: note("stopped") };
  try {
    await f.q.submit({ id: "ok", ref: "batch-a", handler: "work", on });
    const ok = grant(await f.q.claim(claim("work", 1)));
    await f.q.complete({ lease: ok.lease, requestId: "ok", result: 1 });
    await f.q.submit({ id: "dead", handler: "work", on });
    const dead = grant(await f.q.claim(claim("work", 2)));
    await f.q.fail({ lease: dead.lease, requestId: "dead", error: "no", permanent: true });
    await f.q.submit({ id: "late", handler: "work", timeoutMs: 10, on });
    f.advance(10);
    await f.owner.tick();
    await f.q.submit({ id: "stopped", handler: "work", on });
    expect(await f.q.terminate({ job: "stopped" }, { cause: "not needed" })).toEqual({ schedules: 0, runs: 1 });
    await expect(f.q.terminate({ job: "absent" }, { cause: "x" })).rejects.toMatchObject({ code: "NOT_FOUND" });
    await f.restart();
    const notes = await followUps(f.q);
    expect(notes.map((r) => [r.cause!.job, r.cause!.state, (r.payload as { kind: string }).kind, r.state]).sort()).toEqual([
      ["dead", "dead", "dead", "ready"],
      ["late", "timedOut", "late", "ready"],
      ["ok", "succeeded", "ok", "ready"],
      ["stopped", "terminated", "stopped", "ready"],
    ]);
    const okNote = notes.find((r) => r.cause!.job === "ok")!;
    expect([okNote.handler, okNote.ref, okNote.jobId, okNote.scheduleId, okNote.on]).toEqual(["notify", "batch-a", null, null, null]);
    expect(okNote.cause!.run).toBe(ok.run.id);
    // A worker gets the follow-up like any run, with the cause.
    const delivered = grant(await f.q.claim(claim("notify", 3)));
    expect(delivered.run.cause).not.toBeNull();
    await f.q.complete({ lease: delivered.lease, requestId: "noted" });
    // A follow-up cannot carry follow-ups of its own.
    await expect(f.q.submit({ id: "nested", handler: "work", on: { succeeded: { handler: "n", on } } as never })).rejects.toMatchObject({ code: "INVALID_ARGUMENT" });
    // A ref stops its jobs; a stop of the whole scope ends jobs but leaves every follow-up, its own ones included.
    await f.q.submit({ id: "b1", ref: "batch-b", handler: "work" });
    await f.q.submit({ id: "b2", ref: "batch-b", handler: "work" });
    expect(await f.q.terminate({ ref: "batch-b" }, { cause: "batch cancelled" })).toEqual({ schedules: 0, runs: 2 });
    expect(await f.q.getJob("b2")).toMatchObject({ state: "terminated", terminationCause: "batch cancelled" });
    await f.q.submit({ id: "last", handler: "work", on });
    const pending = (await followUps(f.q)).filter((r) => r.state === "ready").length;
    expect(await f.q.terminate({ all: true }, { cause: "maintenance" })).toEqual({ schedules: 0, runs: 1 });
    const after = await followUps(f.q);
    expect(after.filter((r) => r.state === "ready")).toHaveLength(pending + 1);
    expect(after.find((r) => r.cause!.job === "last")?.cause?.state).toBe("terminated");
    // One follow-up still ends by its run ID.
    const one = after.find((r) => r.state === "ready")!;
    expect(await f.q.terminate({ run: one.id }, { cause: "quiet" })).toEqual({ schedules: 0, runs: 1 });
  } finally {
    await f.close();
  }
});

test("poolHandlers runs jobs as defuss-multicore tasks in worker threads: a result, a failure, and a cancel on terminate", async () => {
  const directory = await mkdtemp(join(tmpdir(), "defuss-scheduler-jobs-"));
  const owner = await openScheduler({ directory, tickMs: 10, leaseMs: 600 });
  const q = owner.scope(scope);
  const pool = createPool<typeof tasks>({
    worker: () => new Worker(new URL("./fixtures/pool-worker.ts", import.meta.url), { type: "module" }),
    maxWorkers: 2,
  });
  const worker = startWorker(q, { workerId: "pool", concurrency: 2, handlers: poolHandlers(pool, ["square", "fail", "hang"]) });
  try {
    await q.submit({ id: "hang", handler: "hang" });
    await q.submit({ id: "square", handler: "square", payload: { n: 7 } });
    await q.submit({ id: "fail", handler: "fail", retry: { maxAttempts: 1 } });
    await until(async () => (await q.getJob("square"))?.state === "succeeded" && (await q.getJob("fail"))?.state === "dead");
    expect((await q.getJob("square"))?.result).toEqual({ square: 49 });
    expect((await q.getJob("fail"))?.lastError).toContain("pool task failed");
    expect((await q.getJob("hang"))?.state).toBe("leased");
    await q.terminate({ job: "hang" }, { cause: "stop" });
    await until(async () => pool.stats.cancelled === 1);
  } finally {
    await worker.stop();
    await pool.terminate();
    await owner.close();
    await rm(directory, { recursive: true, force: true });
  }
}, 30_000);
