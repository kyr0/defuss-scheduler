/** VERIFIED: Concurrent real storage requests and HTTP calls exercise lease exclusion, calendars, revisions, retry, tenant isolation, refs, hard stops, histories and what retention removes. */
import { test, expect } from "bun:test";
import { strict as assert } from "node:assert";
import { mkdtemp, readdir, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  openScheduler,
  nextOccurrence,
  startWorker,
  PermanentFailure,
  type Claim,
  type Grant,
  type SchedulerOptions,
} from "../src/index.js";
import { createWorkerEndpoint, connectWorker } from "../src/http.js";
import { openDatabase } from "defuss-db";
import { createFileBackend } from "defuss-dbsonl/server";
const scope = { app: "studio", tenant: "é/42", bucket: "cron" };
const base = {
  id: "a",
  cron: "* * * * * *",
  handler: "build",
  payload: { a: 1 },
};
const claim = (i = 0, seq = 1): Claim => ({
  workerId: "same",
  sessionId: "session",
  slot: i,
  sequence: seq,
  requestId: "c" + seq,
  handlers: ["build"],
});
function grant(value: unknown): Grant {
  assert.equal((value as Grant).kind, "granted");
  return value as Grant;
}
async function fixture(opts: Partial<SchedulerOptions> = {}) {
  let now = Date.UTC(2026, 0, 1);
  const directory = await mkdtemp(join(tmpdir(), "defuss-scheduler-"));
  const options = {
    directory,
    autoTick: false,
    clock: () => now,
    leaseMs: 100,
    retentionMs: 2000,
    ...opts,
  };
  let owner = await openScheduler(options);
  return {
    directory,
    q: owner.scope(scope),
    get owner() {
      return owner;
    },
    advance(ms: number) {
      now += ms;
    },
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
test("calendar dialect, Berlin gap and fold, OR rule, leap day, validation", () => {
  function next(expr: string, ref: string, z = "UTC") {
    return new Date(nextOccurrence(expr, z, new Date(ref))!).toISOString();
  }
  expect(next("30 2 * * *", "2026-03-28T23:00Z", "Europe/Berlin")).toStartWith(
    "2026-03-29T01:00",
  );
  expect(next("30 2 * * *", "2026-10-24T23:00Z", "Europe/Berlin")).toStartWith(
    "2026-10-25T00:30",
  );
  expect(next("30 2 * * *", "2026-10-25T00:30Z", "Europe/Berlin")).toStartWith(
    "2026-10-26T01:30",
  );
  expect(
    next("*/30 * * * *", "2026-10-25T00:30Z", "Europe/Berlin"),
  ).toStartWith("2026-10-25T01:00");
  expect(next("0 0 13 * 1", "2026-11-09T00:00Z")).toStartWith(
    "2026-11-13T00:00",
  );
  expect(next("0 0 29 2 *", "2026-01-01T00:00Z")).toStartWith(
    "2028-02-29T00:00",
  );
  expect(next("*/5 * * * * *", "2026-01-01T00:00:05Z")).toStartWith(
    "2026-01-01T00:00:10",
  );
  for (const expr of [
    "@hourly",
    "* * * *",
    "90 * * * *",
    "9-2 * * * *",
    "*/0 * * * *",
    "* * * * MON",
    "* * * * * * 2027",
  ])
    expect(() => nextOccurrence(expr, "UTC", 0)).toThrow();
  expect(() => nextOccurrence("* * * * *", "bad/zone", 0)).toThrow();
});
test("100 competing pulls, durable locks, scope isolation, replay, stale renew/fail/finish", async () => {
  const f = await fixture();
  try {
    await expect(
      openScheduler({ directory: f.directory }),
    ).rejects.toMatchObject({ code: "OWNER_BUSY" });
    const a = await f.q.create(
      { ...base, concurrencyKey: "db" },
      { requestId: "create" },
    );
    expect(
      (
        await f.q.create(
          { ...base, concurrencyKey: "db" },
          { requestId: "create" },
        )
      ).revision,
    ).toBe(a.revision);
    await expect(
      f.q.create({ ...base, id: "b" }, { requestId: "create" }),
    ).rejects.toMatchObject({ code: "REVISION_CONFLICT" });
    await f.q.create({ ...base, id: "b", concurrencyKey: "db" });
    f.advance(1000);
    await f.owner.tick();
    const claims = await Promise.all(
      Array.from({ length: 100 }, (_, i) => f.q.claim(claim(i))),
    );
    const winners = claims.filter((v) => v.kind === "granted");
    expect(winners).toHaveLength(1);
    const first = winners[0] as Grant;
    expect(grant(await f.q.claim(claim(first.lease.slot))).lease.token).toBe(
      first.lease.token,
    );
    const other = f.owner.scope({ ...scope, tenant: "other" });
    expect(await other.getRun(first.run.id)).toBeNull();
    expect(
      (await other.complete({ lease: first.lease, requestId: "f" })).kind,
    ).toBe("unknown");
    await f.restart();
    const before = await f.q.renew({ lease: first.lease, requestId: "r" });
    expect(before.kind).toBe("applied");
    expect((await f.q.renew({ lease: first.lease, requestId: "r" })).kind).toBe(
      "alreadyApplied",
    );
    f.advance(100);
    expect(
      (await f.q.complete({ lease: first.lease, requestId: "expired" })).kind,
    ).toBe("stale");
    const second = grant(await f.q.claim(claim(first.lease.slot, 2)));
    expect(second.lease.fence).toBe("2");
    for (const op of ["renew", "complete", "fail"] as const) {
      const input = {
        lease: first.lease,
        requestId: op,
        ...(op === "fail" ? { error: "old" } : {}),
      };
      const status =
        op === "fail"
          ? await f.q.fail({ ...input, error: "old" })
          : await f.q[op](input);
      expect(status.kind).toBe("stale");
    }
    expect(
      (await f.q.complete({ lease: second.lease, requestId: "ok", result: 7 }))
        .kind,
    ).toBe("applied");
    expect(
      (await f.q.complete({ lease: second.lease, requestId: "ok", result: 7 }))
        .kind,
    ).toBe("alreadyApplied");
    expect((await f.q.getRun(second.run.id))?.result).toBe(7);
    const older = await f.q.claim(claim(first.lease.slot));
    expect(older.kind).toBe("superseded");
  } finally {
    await f.close();
  }
});
test("catch-up edit barrier, full queue, captured payload, pause/resume/delete and cursor isolation", async () => {
  const f = await fixture({ pageSize: 1, maxPending: 1 });
  try {
    const a = await f.q.create({ ...base, misfire: "catchUp" });
    f.advance(3000);
    const change = { revision: a.revision, requestId: "edit" };
    await expect(
      f.q.update("a", { payload: "new" }, change),
    ).rejects.toMatchObject({ code: "BUSY" });
    for (let i = 1; i <= 3; i++) {
      const job = grant(await f.q.claim(claim(0, i)));
      expect(job.run.payload).toEqual({ a: 1 });
      expect(job.run.scheduledAt).toBe(f.now() - (3 - i) * 1000);
      await f.q.complete({ lease: job.lease, requestId: "done" });
      for (let j = 0; j < 2; j++) await f.owner.tick();
    }
    const edited = await f.q.update("a", { payload: "new" }, change);
    expect(edited.revision).not.toBe(a.revision);
    await expect(
      f.q.pause("a", { revision: a.revision, requestId: "stale" }),
    ).rejects.toMatchObject({ code: "REVISION_CONFLICT" });
    const paused = await f.q.pause("a", {
      revision: edited.revision,
      requestId: "p",
    });
    f.advance(8000);
    await f.owner.tick();
    const resumed = await f.q.resume("a", {
      revision: paused.revision,
      requestId: "r",
    });
    f.advance(1000);
    await f.owner.tick();
    expect(grant(await f.q.claim(claim(0, 4))).run.payload).toBe("new");
    const page = await f.q.listRuns({ limit: 1 });
    expect(page.cursor).toBeTruthy();
    const next = await f.q.listRuns({ limit: 1, cursor: page.cursor! });
    expect(next.items[0].id).not.toBe(page.items[0].id);
    await expect(
      f.owner
        .scope({ ...scope, bucket: "other" })
        .listRuns({ cursor: page.cursor! }),
    ).rejects.toThrow();
    const removed = await f.q.remove("a", {
      revision: resumed.revision,
      requestId: "del",
    });
    expect(removed.state).toBe("removed");
    expect((await f.q.list()).items[0].state).toBe("removed");
    await expect(f.q.create(base)).rejects.toMatchObject({
      code: "ALREADY_EXISTS",
    });
  } finally {
    await f.close();
  }
});
test("latest and skip, capabilities across scan pages, max attempts, durable expiry and cleanup", async () => {
  const f = await fixture({ pageSize: 2 });
  try {
    await f.q.create({ ...base, id: "skip", misfire: "skip", graceMs: 100 });
    for (let i = 0; i < 4; i++)
      await f.q.create({
        ...base,
        id: "x" + i,
        capabilities: ["gpu"],
        retry: { maxAttempts: 1 },
      });
    await f.q.create({
      ...base,
      id: "eligible",
      parallel: true,
      retry: { baseMs: 0, maxAttempts: 2 },
    });
    f.advance(5700);
    for (let n = 0; n < 6; n++) await f.owner.tick();
    expect((await f.q.get("skip"))?.lastSkipped).not.toBeNull();
    expect((await f.q.listRuns()).items).toHaveLength(5);
    expect((await f.q.claim({ ...claim(0), maxPayloadBytes: 0 })).kind).toBe(
      "empty",
    );
    let selected: Grant | undefined,
      seq = 1;
    for (let n = 0; n < 5 && !selected; n++) {
      const g = await f.q.claim(claim(0, ++seq));
      if (g.kind === "granted") selected = g;
    }
    assert.ok(selected);
    expect(selected.run.scheduleId).toBe("eligible");
    await f.q.fail({
      lease: selected.lease,
      requestId: "fail",
      error: "temporary",
    });
    let retry: Grant | undefined;
    for (let i = 0; i < 5 && !retry; i++) {
      const g = await f.q.claim(claim(0, ++seq));
      if (g.kind === "granted") retry = g;
    }
    assert.ok(retry);
    expect(retry.run.id).toBe(selected.run.id);
    expect(retry.run.attempt).toBe(2);
    f.advance(100);
    await f.owner.tick();
    expect((await f.q.getRun(retry.run.id))?.state).toBe("dead");
    const gpu = grant(await f.q.claim({ ...claim(9), capabilities: ["gpu"] }));
    await f.q.fail({
      lease: gpu.lease,
      requestId: "f",
      error: "permanent",
      permanent: true,
    });
    expect((await f.q.getRun(gpu.run.id))?.state).toBe("dead");
    f.advance(3000);
    for (let i = 0; i < 15; i++) await f.owner.tick();
    expect(await f.q.getRun(retry.run.id)).toBeNull();
  } finally {
    await f.close();
  }
});
test("worker over real HTTP, renewal during a slow handler, permanent failure and graceful drain", async () => {
  const f = await fixture({
    clock: undefined,
    autoTick: true,
    tickMs: 10,
    pollMs: 5,
    leaseMs: 250,
  });
  const endpoint = createWorkerEndpoint((req) =>
    req.headers.get("authorization") === "Bearer test"
      ? {
          workerId: "client",
          handlers: ["build", "reject"],
          port: f.q,
        }
      : null,
  );
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: endpoint });
  let worker: ReturnType<typeof startWorker> | undefined;
  try {
    const port = connectWorker(server.url.href, { token: "test" });
    await expect(
      connectWorker(server.url.href, { token: "wrong" }).claim(claim()),
    ).rejects.toThrow();
    await expect(port.claim(claim())).rejects.toThrow();
    await f.q.create(base);
    await f.q.create({ ...base, id: "bad", handler: "reject" });
    worker = startWorker(port, {
      workerId: "client",
      concurrency: 2,
      handlers: {
        build: async (_run, ctx) => {
          await Bun.sleep(550);
          assert.ok(!ctx.signal.aborted);
          return 9;
        },
        reject: () => {
          throw new PermanentFailure("invalid");
        },
      },
    });
    const until = Date.now() + 6000;
    let states: RunState[] = [];
    type RunState = { state: string; attempt: number };
    while (Date.now() < until) {
      states = (await f.q.listRuns()).items;
      if (
        states.some((r) => r.state === "succeeded") &&
        states.some((r) => r.state === "dead")
      )
        break;
      await Bun.sleep(30);
    }
    expect(
      states.some((r) => r.state === "succeeded" && r.attempt === 1),
    ).toBeTrue();
    expect(
      states.some((r) => r.state === "dead" && r.attempt === 1),
    ).toBeTrue();
  } finally {
    await worker?.stop();
    await server.stop(true);
    await f.close();
  }
});
test("parallel and isolated groups, retry with same worker identity, compaction and closed admission", async () => {
  const f = await fixture();
  try {
    const peer = f.owner.scope({ ...scope, tenant: "other" });
    await peer.create(base);
    await f.q.create({
      ...base,
      parallel: true,
      misfire: "catchUp",
      retry: { baseMs: 0 },
    });
    f.advance(2000);
    await f.owner.tick();
    const a = grant(await f.q.claim(claim(0))),
      b = grant(await f.q.claim(claim(1)));
    expect(a.run.id).not.toBe(b.run.id);
    expect(grant(await peer.claim(claim(0))).run.id).not.toBe(a.run.id);
    await f.q.complete({ lease: b.lease, requestId: "done" });
    f.advance(100);
    await f.owner.tick();
    const c = grant(await f.q.claim(claim(0, 2)));
    expect(c.run.id).toBe(a.run.id);
    expect(c.lease.fence).toBe("2");
    expect(
      (await f.q.complete({ lease: a.lease, requestId: "old" })).kind,
    ).toBe("stale");
    await f.owner.compact();
    await f.restart();
    expect((await f.q.getRun(b.run.id))?.state).toBe("succeeded");
    await f.owner.close();
    await expect(f.q.get("a")).rejects.toMatchObject({ code: "CLOSED" });
  } finally {
    await f.close();
  }
});
test("invalid cron, group edits, rejected values and typed handler", async () => {
  const { defineHandler } = await import("../src/worker.js");
  const handler = defineHandler(
    (v) => {
      if (typeof v !== "number") throw Error("number required");
      return v;
    },
    (v) => v + 1,
  );
  expect(() => handler({ payload: "bad" } as never, {} as never)).toThrow(
    PermanentFailure,
  );
  expect(handler({ payload: 7 } as never, {} as never)).toBe(8);
  const f = await fixture();
  try {
    await expect(f.q.create({ ...base, cron: "0 0 31 2 *" })).rejects.toThrow();
    await expect(
      f.q.create({ ...base, concurrencyKey: "shared", parallel: true }),
    ).rejects.toThrow();
    await expect(f.q.create({ ...base, payload: NaN })).rejects.toThrow();
    const a = await f.q.create(base);
    await expect(
      f.q.update("a", { parallel: true } as never, {
        requestId: "p",
        revision: a.revision,
      }),
    ).rejects.toThrow();
    await expect(f.q.claim({ ...claim(), handlers: [] })).rejects.toThrow();
  } finally {
    await f.close();
  }
});
test("histories record every status and edit without heartbeats or cursor moves, and retention erases a run's", async () => {
  const f = await fixture({ retentionMs: 1000 });
  try {
    const before = Date.now();
    const created = await f.q.create({ ...base, retry: { baseMs: 0 } });
    expect(created.createdAt).toBeGreaterThanOrEqual(before);
    expect(created.updatedAt).toBe(created.createdAt);
    f.advance(1000);
    await f.owner.tick();
    const first = grant(await f.q.claim(claim(0, 1)));
    expect(first.run.updatedAt).toBeGreaterThanOrEqual(first.run.createdAt);
    for (const requestId of ["r1", "r2"])
      expect((await f.q.renew({ lease: first.lease, requestId })).kind).toBe(
        "applied",
      );
    await f.q.fail({ lease: first.lease, requestId: "f", error: "flaky" });
    const second = grant(await f.q.claim(claim(0, 2)));
    await f.q.complete({ lease: second.lease, requestId: "ok", result: 1 });
    const done = await f.q.getRun(first.run.id);
    expect(done?.createdAt).toBe(first.run.createdAt);
    const states = async () =>
      (await f.q.runHistory(first.run.id)).map((v) => v.value?.state ?? null);
    expect(await states()).toEqual([
      "succeeded",
      "leased",
      "retryWait",
      "leased",
      "ready",
    ]);
    const timeline = await f.q.runHistory(first.run.id);
    expect(timeline[0].until).toBeNull();
    expect(timeline[0].value?.updatedAt).toBe(timeline[0].from);
    expect(timeline[1].until).toBe(timeline[0].from);
    expect(timeline[2].value?.lastError).toBe("flaky");
    expect(await f.q.runHistory(first.run.id, { limit: 2 })).toHaveLength(2);

    // The calendar cursor moved with the occurrence; only the edit is a second version.
    expect(await f.q.scheduleHistory("a")).toHaveLength(1);
    const edited = await f.q.update(
      "a",
      { payload: "new" },
      { revision: created.revision, requestId: "edit" },
    );
    expect(edited.createdAt).toBe(created.createdAt);
    expect(edited.updatedAt).toBeGreaterThanOrEqual(created.updatedAt);
    const versions = await f.q.scheduleHistory("a");
    expect(versions.map((v) => v.value?.payload)).toEqual(["new", { a: 1 }]);
    expect(versions[0].value).not.toHaveProperty("nextDue");

    // Retention removes the run with its history. The schedule's history stays.
    f.advance(2000);
    for (let i = 0; i < 3; i++) await f.owner.tick();
    expect(await f.q.getRun(first.run.id)).toBeNull();
    expect(await states()).toEqual([]);
    await f.restart();
    expect(await states()).toEqual([]);
    expect(await f.q.scheduleHistory("a")).toHaveLength(2);
    await expect(
      f.q.runHistory(first.run.id, { limit: 0 }),
    ).rejects.toMatchObject({ code: "INVALID_ARGUMENT" });
  } finally {
    await f.close();
  }
});
test("idle polls leave the store files untouched and a sequence acquires one job at most", async () => {
  const f = await fixture();
  async function bytes() {
    let total = 0;
    for (const entry of await readdir(f.directory, {
      recursive: true,
      withFileTypes: true,
    }))
      if (entry.isFile())
        total += (await stat(join(entry.parentPath, entry.name))).size;
    return total;
  }
  const idle = { kind: "empty", retryAfterMs: 250 };
  try {
    await f.q.create(base);
    const before = await bytes();
    for (let seq = 1; seq <= 40; seq++)
      expect(await f.q.claim(claim(0, seq))).toEqual(idle);
    for (let i = 0; i < 5; i++) await f.owner.tick();
    expect(await bytes()).toBe(before);

    f.advance(1000);
    await f.owner.tick();
    const job = grant(await f.q.claim(claim(0, 41)));
    expect(grant(await f.q.claim(claim(0, 41))).lease.token).toBe(
      job.lease.token,
    );
    await f.q.complete({ lease: job.lease, requestId: "done" });
    // The finished grant is not handed out again, and an earlier sequence stays behind it.
    expect((await f.q.claim(claim(0, 41))).kind).toBe("superseded");
    expect((await f.q.claim(claim(0, 40))).kind).toBe("superseded");
    // The first empty answer after a run drops the slot's reference to it; the ones after it write nothing.
    expect(await f.q.claim(claim(0, 42))).toEqual(idle);
    const settled = await bytes();
    for (let seq = 43; seq <= 80; seq++)
      expect(await f.q.claim(claim(0, seq))).toEqual(idle);
    expect(await bytes()).toBe(settled);
    await f.restart();
    expect((await f.q.claim(claim(0, 41))).kind).toBe("superseded");
    expect(await f.q.claim(claim(0, 81))).toEqual(idle);
  } finally {
    await f.close();
  }
});
test("a slot record and a parallel run's group last one retention period, a held lease longer", async () => {
  const f = await fixture();
  try {
    await f.q.create({ ...base, parallel: true });
    f.advance(1000);
    await f.owner.tick();
    const held = grant(await f.q.claim(claim(0)));
    // Renewals do not write the slot record, yet it outlives the retention period while its lease is held.
    for (let i = 0; i < 30; i++) {
      f.advance(90);
      const renewal = { lease: held.lease, requestId: "renew" + i };
      expect((await f.q.renew(renewal)).kind).toBe("applied");
      await f.owner.tick();
    }
    expect(grant(await f.q.claim(claim(0))).lease.token).toBe(held.lease.token);
    await f.q.complete({ lease: held.lease, requestId: "done" });
    expect((await f.q.claim(claim(0))).kind).toBe("superseded");
    await f.q.terminate({ all: true }, { cause: "drain" });

    f.advance(5000);
    for (let i = 0; i < 5; i++) await f.owner.tick();
    // The slot's memory of its sequences is gone with the record, so the old claim is a new one.
    expect((await f.q.claim(claim(0))).kind).toBe("empty");
    expect(await f.q.runHistory(held.run.id)).toEqual([]);
    await f.owner.close();
    const db = await openDatabase(await createFileBackend(f.directory), {
      name: "defuss_scheduler",
      create: false,
    });
    try {
      const { rows } = await db
        .collection("ledger")
        .pageIdsByIndex("key", { limit: 100 });
      // What stays is per schedule and per scope: no group, slot, receipt or index entry of a finished run.
      expect(rows.map((id) => id.split("/")[0])).toEqual([
        "!leaseTime",
        "!readyByHandler",
        "!version",
        "c",
        "n",
      ]);
      const runs = db.collection("runs");
      expect((await runs.pageIdsByIndex("key", { limit: 1 })).rows).toEqual([]);
    } finally {
      await db.close();
    }
  } finally {
    await f.close();
  }
});
test("a retention backlog is collected without a tick of waiting per page", async () => {
  let now = Date.UTC(2026, 0, 1);
  const directory = await mkdtemp(join(tmpdir(), "defuss-scheduler-"));
  const owner = await openScheduler({
    directory,
    clock: () => now,
    tickMs: 1000,
    pageSize: 1,
    retentionMs: 1000,
  });
  try {
    const q = owner.scope(scope);
    await q.create({ ...base, parallel: true, misfire: "catchUp" });
    now += 5000;
    const ids: string[] = [];
    for (let seq = 1; seq <= 5; seq++) {
      await owner.tick();
      const job = grant(await q.claim(claim(0, seq)));
      await q.complete({ lease: job.lease, requestId: "done" });
      ids.push(job.run.id);
    }
    now += 2000;
    // Each finished run left a receipt and its own entry. One entry per turn and one turn per second
    // would keep the last of them for more than ten seconds.
    const deadline = Date.now() + 4000;
    let left = ids.length;
    while (left && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 50));
      const runs = await Promise.all(ids.map((id) => q.getRun(id)));
      left = runs.filter(Boolean).length;
    }
    expect(left).toBe(0);
  } finally {
    await owner.close();
    await rm(directory, { recursive: true, force: true });
  }
});
test("a ref names schedules and their runs across schedule IDs", async () => {
  const f = await fixture({ retentionMs: 1000 });
  try {
    const first = await f.q.create({ ...base, id: "report-1", ref: "report" });
    expect(first.ref).toBe("report");
    await f.q.create({ ...base, id: "other" });
    f.advance(1000);
    await f.owner.tick();
    await f.q.remove("report-1", {
      revision: first.revision,
      requestId: "rm",
    });
    // The removed ID stays reserved. Its ref moves on to a successor.
    await expect(
      f.q.create({ ...base, id: "report-1", ref: "report" }),
    ).rejects.toMatchObject({ code: "ALREADY_EXISTS" });
    const successor = await f.q.create({
      ...base,
      id: "report-2",
      ref: "report",
    });
    await expect(
      f.q.update("report-2", { ref: "renamed" } as never, {
        revision: successor.revision,
        requestId: "rename",
      }),
    ).rejects.toMatchObject({ code: "INVALID_ARGUMENT" });
    f.advance(1000);
    await f.owner.tick();

    const named = (await f.q.list({ ref: "report" })).items;
    expect(named.map((s) => [s.id, s.state]).sort()).toEqual([
      ["report-1", "removed"],
      ["report-2", "active"],
    ]);
    expect((await f.q.list()).items).toHaveLength(3);
    // Runs of one ref come in scheduled order, whichever schedule admitted them.
    const runs = await f.q.listRuns({ ref: "report" });
    expect(runs.items.map((r) => [r.scheduleId, r.ref])).toEqual([
      ["report-1", "report"],
      ["report-2", "report"],
    ]);
    expect(runs.items[0].scheduledAt).toBeLessThan(runs.items[1].scheduledAt);
    const page = await f.q.listRuns({ ref: "report", limit: 1 });
    expect(page.items[0].scheduleId).toBe("report-1");
    const rest = await f.q.listRuns({
      ref: "report",
      limit: 1,
      cursor: page.cursor!,
    });
    expect(rest.items[0].scheduleId).toBe("report-2");
    expect(rest.cursor).toBeNull();
    await expect(
      f.q.listRuns({ cursor: page.cursor! }),
    ).rejects.toMatchObject({ code: "INVALID_ARGUMENT" });
    expect(await f.q.listRuns({ ref: "unknown" })).toEqual({
      items: [],
      cursor: null,
    });
    await expect(f.q.list({ ref: "" })).rejects.toMatchObject({
      code: "INVALID_ARGUMENT",
    });
    expect((await f.q.scheduleHistory("report-2"))[0].value?.ref).toBe(
      "report",
    );

    // Retention removes a finished run and its ref entry together.
    await f.q.pause("report-2", {
      revision: successor.revision,
      requestId: "quiet",
    });
    const old = grant(
      await f.q.claim({ ...claim(0, 1), maxPayloadBytes: 1000 }),
    );
    await f.q.complete({ lease: old.lease, requestId: "done" });
    f.advance(2000);
    for (let i = 0; i < 3; i++) await f.owner.tick();
    expect(await f.q.getRun(old.run.id)).toBeNull();
    const kept = (await f.q.listRuns({ ref: "report" })).items;
    expect(kept.map((r) => r.id)).not.toContain(old.run.id);
    expect(kept.length + (old.run.ref === "report" ? 1 : 0)).toBe(2);
  } finally {
    await f.close();
  }
});
test("a hard stop ends a backlog an edit waits for and notes status and cause", async () => {
  const f = await fixture({ pageSize: 2, retentionMs: 60000 });
  try {
    const a = await f.q.create({
      ...base,
      ref: "nightly",
      misfire: "catchUp",
      parallel: true,
    });
    f.advance(7000);
    await f.owner.tick();
    const held = grant(await f.q.claim(claim(0, 1)));
    // Seven occurrences are overdue. The edit admits one more page and keeps draining.
    const edit = { revision: a.revision, requestId: "pause" };
    await expect(f.q.pause("a", edit)).rejects.toMatchObject({ code: "BUSY" });

    // Selectively: one ready run, then the schedule with the rest of its runs, the leased one included.
    const ready = (await f.q.listRuns()).items.find((r) => r.state === "ready")!;
    expect(
      await f.q.terminate({ run: ready.id }, { cause: "bad input" }),
    ).toEqual({ schedules: 0, runs: 1 });
    expect(await f.q.getRun(ready.id)).toMatchObject({
      state: "terminated",
      terminationCause: "bad input",
      finishedAt: f.now(),
    });
    expect(await f.q.terminate({ run: ready.id }, { cause: "twice" })).toEqual(
      { schedules: 0, runs: 0 },
    );
    expect(
      await f.q.terminate({ schedule: "a" }, { cause: "runaway" }),
    ).toEqual({ schedules: 1, runs: 3 });
    expect(await f.q.get("a")).toMatchObject({
      state: "terminated",
      terminationCause: "runaway",
      nextDue: null,
    });
    const runs = (await f.q.listRuns()).items;
    expect(runs.map((r) => r.state)).toEqual(Array(4).fill("terminated"));
    expect(runs.map((r) => r.terminationCause).sort()).toEqual([
      "bad input",
      "runaway",
      "runaway",
      "runaway",
    ]);
    // The lease ended with its run, and both histories show the stop.
    for (const op of ["renew", "complete"] as const)
      expect((await f.q[op]({ lease: held.lease, requestId: op })).kind).toBe(
        "stale",
      );
    expect(
      (await f.q.runHistory(held.run.id)).map((v) => v.value?.state),
    ).toEqual(["terminated", "leased", "ready"]);
    expect(
      (await f.q.scheduleHistory("a")).map((v) => v.value?.state),
    ).toEqual(["terminated", "active"]);

    // Nothing is admitted or granted afterwards, and the schedule is as final as a removed one.
    f.advance(3000);
    for (let i = 0; i < 3; i++) await f.owner.tick();
    expect((await f.q.claim(claim(0, 2))).kind).toBe("empty");
    expect((await f.q.listRuns()).items).toHaveLength(4);
    await expect(f.q.pause("a", edit)).rejects.toMatchObject({
      code: "NOT_FOUND",
    });
    await expect(f.q.create(base)).rejects.toMatchObject({
      code: "ALREADY_EXISTS",
    });
    expect(await f.q.terminate({ schedule: "a" }, { cause: "twice" })).toEqual(
      { schedules: 0, runs: 0 },
    );

    // Collectively: every schedule that carries a ref, then the rest of the scope. Other scopes stay untouched.
    await f.q.create({ ...base, id: "b", ref: "nightly" });
    await f.q.create({
      ...base,
      id: "c",
      ref: "nightly",
      misfire: "catchUp",
      parallel: true,
    });
    const d = await f.q.create({ ...base, id: "d" });
    const peer = f.owner.scope({ ...scope, tenant: "other" });
    await peer.create({ ...base, ref: "nightly" });
    f.advance(2000);
    for (let i = 0; i < 3; i++) await f.owner.tick();
    await f.q.remove("d", { revision: d.revision, requestId: "rm" });
    expect(
      await f.q.terminate({ ref: "nightly" }, { cause: "maintenance" }),
    ).toEqual({ schedules: 2, runs: 3 });
    expect(
      (await f.q.list({ ref: "nightly" })).items.map((s) => s.terminationCause),
    ).toEqual(["runaway", "maintenance", "maintenance"]);
    // A removed schedule keeps its state; the run it had admitted ends with the scope.
    expect(await f.q.terminate({ all: true }, { cause: "shutdown" })).toEqual({
      schedules: 0,
      runs: 1,
    });
    expect((await f.q.get("d"))?.state).toBe("removed");
    expect(
      (await f.q.listRuns({ limit: 500 })).items.every(
        (r) => r.state === "terminated",
      ),
    ).toBeTrue();
    expect(grant(await peer.claim(claim(0, 1))).run.ref).toBe("nightly");

    for (const [target, options] of [
      [{ run: "x", all: true }, { cause: "two targets" }],
      [{}, { cause: "no target" }],
      [{ all: false }, { cause: "not all" }],
      [{ all: true }, { cause: "" }],
      [{ all: true }, {}],
    ])
      await expect(
        f.q.terminate(target as never, options as never),
      ).rejects.toMatchObject({ code: "INVALID_ARGUMENT" });
    for (const target of [{ run: "absent" }, { schedule: "absent" }])
      await expect(
        f.q.terminate(target, { cause: "missing" }),
      ).rejects.toMatchObject({ code: "NOT_FOUND" });
  } finally {
    await f.close();
  }
});
test("a claim between a hard stop and its sweep is granted nothing", async () => {
  const f = await fixture({ pageSize: 2 });
  try {
    await f.q.create({ ...base, misfire: "catchUp", parallel: true });
    f.advance(2000);
    await f.owner.tick();
    // Commands run in call order: the stop marks the schedule, the claim follows, the sweep comes last.
    const stopping = f.q.terminate({ schedule: "a" }, { cause: "guard" });
    expect((await f.q.claim(claim(0, 1))).kind).toBe("empty");
    expect(await stopping).toEqual({ schedules: 1, runs: 0 });
    const runs = (await f.q.listRuns()).items;
    expect(runs.map((r) => [r.state, r.terminationCause, r.attempt])).toEqual([
      ["terminated", "guard", 0],
      ["terminated", "guard", 0],
    ]);
  } finally {
    await f.close();
  }
});
test("a hard stop aborts the handler of a leased run at its next heartbeat", async () => {
  const f = await fixture({
    clock: undefined,
    autoTick: true,
    tickMs: 10,
    pollMs: 5,
    leaseMs: 250,
  });
  let worker: ReturnType<typeof startWorker> | undefined;
  try {
    await f.q.create(base);
    const aborted = new Promise<string>((resolve) => {
      worker = startWorker(f.q, {
        workerId: "local",
        handlers: {
          build: (run, ctx) =>
            new Promise((_, reject) => {
              ctx.signal.addEventListener("abort", () => {
                resolve(run.id);
                reject(new Error("stopped"));
              });
              void f.q.terminate({ run: run.id }, { cause: "operator" });
            }),
        },
      });
    });
    expect(await f.q.getRun(await aborted)).toMatchObject({
      state: "terminated",
      terminationCause: "operator",
      attempt: 1,
    });
  } finally {
    await worker?.stop();
    await f.close();
  }
});

test("ready names every scope that holds a run a claim would grant now, by handler", async () => {
  const f = await fixture();
  try {
    const mail = { app: "studio", tenant: "b", bucket: "cron" };
    const yearly = { app: "studio", tenant: "c", bucket: "cron" };
    await f.q.create(base);
    await f.owner.scope(mail).create({ ...base, handler: "mail" });
    await f.owner.scope(yearly).create({ ...base, cron: "0 0 1 1 *" });
    expect(await f.owner.ready({ handlers: ["build", "mail"] })).toEqual([]);
    f.advance(1000);
    await f.owner.tick();
    expect(await f.owner.ready({ handlers: ["build"] })).toEqual([scope]);
    expect(await f.owner.ready({ handlers: ["build", "mail"], limit: 1 })).toHaveLength(1);
    expect(new Set((await f.owner.ready({ handlers: ["build", "mail"] })).map((s) => s.tenant))).toEqual(
      new Set([scope.tenant, mail.tenant]),
    );
    // A leased run leaves the index; a failed attempt returns once its retry delay has passed.
    const leased = grant(await f.q.claim(claim()));
    expect(await f.owner.ready({ handlers: ["build"] })).toEqual([]);
    await f.q.fail({ lease: leased.lease, requestId: "f1", error: "boom" });
    expect(await f.owner.ready({ handlers: ["build"] })).toEqual([]);
    f.advance(500);
    expect(await f.owner.ready({ handlers: ["build"] })).toEqual([scope]);
    // A hard stop takes the scope's ready runs out of the index.
    await f.owner.scope(mail).terminate({ all: true }, { cause: "test" });
    expect(await f.owner.ready({ handlers: ["mail"] })).toEqual([]);
    await expect(f.owner.ready({ handlers: [] })).rejects.toMatchObject({ code: "INVALID_ARGUMENT" });
    await expect(f.owner.ready({ handlers: ["build"], limit: 0 })).rejects.toMatchObject({ code: "INVALID_ARGUMENT" });
  } finally {
    await f.close();
  }
});

test("opening a 0.1.0 store indexes its ready runs by handler", async () => {
  const f = await fixture();
  try {
    await f.q.create(base);
    f.advance(1000);
    await f.owner.tick();
    await f.owner.close();
    // Remove what 0.1.0 did not write: the entries by handler and the migration marker.
    const db = await openDatabase(await createFileBackend(f.directory), { name: "defuss_scheduler", create: false });
    try {
      const ledger = db.collection("ledger");
      const { rows } = await ledger.pageIdsByIndex("key", { limit: 1000 });
      const added = rows.filter((id) => String(id).startsWith("h/") || id === "!readyByHandler");
      expect(added.length).toBe(2);
      for (const id of added) await ledger.delete(id);
    } finally {
      await db.close();
    }
    await f.restart();
    expect(await f.owner.ready({ handlers: ["build"] })).toEqual([scope]);
    grant(await f.q.claim(claim()));
    expect(await f.owner.ready({ handlers: ["build"] })).toEqual([]);
  } finally {
    await f.close();
  }
});
