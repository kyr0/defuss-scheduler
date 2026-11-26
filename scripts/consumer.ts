/** VERIFIED: A clean installed consumer verifies a killed worker's durable effect and rejects a stale completion after runtime handoff. Its host also serves the browser consumer next to the worker route. */
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { readFile, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";
import { once } from "node:events";
import { setTimeout as sleep } from "node:timers/promises";
import { openScheduler, type Grant } from "defuss-scheduler";
import { connectWorker, createWorkerEndpoint } from "defuss-scheduler/http";
import { poolHandlers, startWorker } from "defuss-scheduler/worker";
import { createPool } from "defuss-multicore";
import { Worker as ThreadWorker } from "node:worker_threads";

const [mode, dir, address] = process.argv.slice(2);
const fixture = JSON.parse(
  await readFile(new URL("./acceptance.json", import.meta.url), "utf8"),
);
const emit = (data: unknown) => console.log(JSON.stringify(data));
if (mode === "host" || mode === "reopen") {
  const owner = await openScheduler({
    directory: dir,
    leaseMs: 1800,
    tickMs: 25,
    pollMs: 15,
  });
  const q = owner.scope(fixture.scope);
  if (mode === "host") {
    const schedule = await q.create(fixture.schedule);
    const end = Date.now() + 5000;
    while (!(await q.listRuns()).items.length && Date.now() < end)
      await sleep(15);
    await q.pause(schedule.id, {
      revision: schedule.revision,
      requestId: "pause",
    });
    assert.equal((await q.listRuns()).items.length, 1);
    // The unclaimed run makes its scope ready for a worker that serves every scope.
    assert.deepEqual(await owner.ready({ handlers: [fixture.schedule.handler] }), [fixture.scope]);
  }
  const handle = createWorkerEndpoint((req) =>
    req.headers.get("authorization") === "Bearer integration"
      ? { port: q, workerId: "machine", handlers: ["rollup"] }
      : null,
  );
  const server = createServer(async (req, res) => {
    try {
      if (req.method === "GET") {
        // One origin for the page and the worker route, as an application would mount them.
        const name = req.url === "/" ? "index.html" : basename(req.url ?? "");
        const file = await readFile(new URL("./web/" + name, import.meta.url));
        res.writeHead(200, {
          "Content-Type": name.endsWith(".js") ? "text/javascript" : "text/html",
        });
        res.end(file);
        return;
      }
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      const out = await handle(
        new Request("http://localhost/worker", {
          method: "POST",
          headers: req.headers as HeadersInit,
          body: Buffer.concat(chunks),
        }),
      );
      res.writeHead(out.status, { "Content-Type": "application/json" });
      res.end(await out.text());
    } catch {
      res.writeHead(500);
      res.end();
    }
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const addr = server.address();
  assert.ok(addr && typeof addr === "object");
  emit({
    url: "http://127.0.0.1:" + addr.port,
    runtime: process.versions.bun ?? process.version,
  });
  process.on("SIGTERM", async () => {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await owner.close();
  });
} else if (mode === "inspect") {
  const owner = await openScheduler({ directory: dir });
  try {
    const q = owner.scope(fixture.scope);
    const runs = await q.listRuns();
    assert.equal(runs.items.length, 1);
    const r = runs.items[0];
    assert.ok(r.createdAt < r.updatedAt);
    const effect = await readFile(join(dir, "effect"), "utf8").catch(() => null);
    // The ref finds the schedule and its run without their IDs, and a hard stop by ref ends the paused schedule.
    const ref: string = fixture.schedule.ref;
    const named = {
      schedules: (await q.list({ ref })).items.map((s) => s.id),
      runs: (await q.listRuns({ ref })).items.map((n) => n.id === r.id),
    };
    const terminated = await q.terminate({ ref }, { cause: "inspected" });
    assert.equal((await q.get(r.scheduleId!))?.terminationCause, "inspected");
    emit({
      named,
      terminated,
      state: r.state,
      attempts: r.attempt,
      result: r.result,
      effectMatchesRun: effect === r.id,
      history: (await q.runHistory(r.id)).map((v) => v.value?.state),
      scheduleStates: (await q.scheduleHistory(r.scheduleId!)).map(
        (v) => v.value?.state,
      ),
    });
  } finally {
    await owner.close();
  }
} else if (mode === "jobs") {
  // A one-off job runs as a defuss-multicore task in a worker thread; its endings admit follow-ups a handler sees.
  const owner = await openScheduler({ directory: dir, leaseMs: 1500, tickMs: 10, pollMs: 10 });
  const q = owner.scope(fixture.scope);
  // VERIFIED by probe: under Bun 1.4.5 the pool never answers through node:worker_threads, but does through Bun's
  // global Worker; Node.js has no global Worker.
  const url = new URL("./pool-worker.mjs", import.meta.url);
  const pool = createPool({
    worker: () => (typeof Worker === "function" ? new Worker(url, { type: "module" }) : new ThreadWorker(url)),
    maxWorkers: 2,
  });
  const causes: string[] = [];
  const worker = startWorker(q, {
    workerId: "jobs",
    concurrency: 2,
    handlers: {
      ...poolHandlers(pool, ["square"]),
      notify: (run) => {
        causes.push(run.cause!.job + " " + run.cause!.state);
        return null;
      },
    },
  });
  try {
    const job = { id: "square-7", handler: "square", payload: { n: 7 }, timeoutMs: 10000, on: { succeeded: { handler: "notify" } } };
    const admitted = await q.submit(job, { requestId: "submit" });
    const replayed = (await q.submit(job, { requestId: "submit" })).id === admitted.id;
    await q.submit({ id: "unclaimed", handler: "nobody", on: { terminated: { handler: "notify" } } });
    const stopped = await q.terminate({ job: "unclaimed" }, { cause: "e2e" });
    const end = Date.now() + 8000;
    while (causes.length < 2 && Date.now() < end) await sleep(15);
    const done = await q.getJob("square-7");
    emit({ state: done?.state, result: done?.result, replayed, stopped, causes: causes.sort() });
  } finally {
    await worker.stop();
    await pool.close();
    await owner.close();
  }
} else {
  assert.equal(typeof startWorker, "function");
  const port = connectWorker(address, { token: "integration" });
  const prior: Grant | undefined =
    mode === "replace"
      ? JSON.parse(await readFile(join(dir, "old-grant"), "utf8"))
      : undefined;
  let sequence = 0;
  const until = Date.now() + 8000;
  for (;;) {
    const g = await port.claim({
      workerId: "machine",
      sessionId: mode,
      slot: 0,
      sequence: ++sequence,
      requestId: "c" + sequence,
      handlers: ["rollup"],
    });
    if (mode === "contender") {
      assert.equal(g.kind, "empty");
      emit({ blocked: true });
      break;
    }
    if (g.kind !== "granted") {
      assert.ok(Date.now() < until);
      await sleep(25);
      continue;
    }
    if (prior) {
      assert.equal(prior.run.id, g.run.id);
      assert.notEqual(prior.lease.token, g.lease.token);
      assert.ok(BigInt(g.lease.fence) > BigInt(prior.lease.fence));
      assert.equal(
        (await port.complete({ lease: prior.lease, requestId: "stale" })).kind,
        "stale",
      );
    }
    let applied = true;
    try {
      await writeFile(join(dir, "effect"), g.run.id, { flag: "wx" });
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code !== "EEXIST") throw e;
      assert.equal(await readFile(join(dir, "effect"), "utf8"), g.run.id);
      applied = false;
    }
    if (mode === "first") {
      assert.ok(applied);
      await writeFile(join(dir, "old-grant"), JSON.stringify(g));
      emit({ applied, fence: g.lease.fence });
      setInterval(() => {}, 60000);
    } else {
      assert.equal(applied, false);
      const input = {
        lease: g.lease,
        requestId: "done",
        result: { changes: 1 },
      };
      assert.equal((await port.complete(input)).kind, "applied");
      assert.equal((await port.complete(input)).kind, "alreadyApplied");
      emit({ applied, fence: g.lease.fence });
    }
    break;
  }
}
