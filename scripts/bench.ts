/** VERIFIED: Each benchmark releases the same count of due runs and measures actual waiting and service using a monotonic clock. */
import { openScheduler, type Grant } from "../dist/index.js";
import { mkdtemp, mkdir, rm, writeFile, statfs } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir, cpus } from "node:os";
import { performance } from "node:perf_hooks";
import { strict as assert } from "node:assert";
const root = await mkdtemp(join(tmpdir(), "scheduler-bench-"));
type Result = {
  workers: number;
  conflicts: number;
  workload: string;
  completed: number;
  claims: number;
  empty: number;
  elapsedMs: number;
  completedPerSec: number;
  claimMs: number[];
  renewMs: number[];
  dispatchLagMs: number[];
  workersWithJobs: number;
  minJobs: number;
  maxJobs: number;
};
function quantiles(values: number[]) {
  values.sort((a, b) => a - b);
  return [0.5, 0.95, 0.99].map((p) =>
    Number((values[Math.ceil(p * values.length) - 1] ?? 0).toFixed(3)),
  );
}
async function measure(workers: number, workload: string): Promise<Result> {
  const dir = join(root, workers + "-" + workload);
  const realStart = performance.now();
  let logical = Date.UTC(2026, 0, 1);
  let conflicts = 0;
  const owner = await openScheduler({
    onTransaction: (m) => {
      conflicts += m.conflicts;
    },
    directory: dir,
    clock: () => logical,
    autoTick: false,
    pageSize: 32,
  });
  const q = owner.scope({ app: "bench", tenant: "local", bucket: "work" });
  const total = 128;
  const claimMs: number[] = [],
    renewMs: number[] = [],
    lag: number[] = [];
  let completed = 0,
    claims = 0,
    empty = 0;
  const activity = Array<number>(workers).fill(0);
  try {
    if (workload === "filtered-prefix") {
      for (let i = 0; i < 64; i++)
        await q.create({
          id: "blocked" + i,
          handler: "other",
          cron: "1 * * * * *",
        });
      logical += 1000;
      for (let i = 0; i < 3; i++) await owner.tick();
    }
    for (let i = 0; i < total; i++)
      await q.create({
        id: "eligible" + i,
        handler: "run",
        cron: "* * * * * *",
        ...(workload === "one-key" && { concurrencyKey: "shared" }),
      });
    logical += 1000;
    for (let i = 0; i < 9; i++) await owner.tick();
    assert.equal(
      (await q.listRuns({ limit: 500 })).items.length,
      total + (workload === "filtered-prefix" ? 64 : 0),
    );
    // All work is due at the barrier. Real elapsed time measures queue wait after release.
    const start = performance.now();
    await Promise.all(
      activity.map(async (_, slot) => {
        let sequence = 0;
        while (completed < total) {
          const a = performance.now();
          const g = await q.claim({
            workerId: "bench",
            sessionId: "batch",
            slot,
            sequence: ++sequence,
            requestId: String(sequence),
            handlers: ["run"],
          });
          claimMs.push(performance.now() - a);
          claims++;
          if (g.kind !== "granted") {
            empty++;
            continue;
          }
          const grant: Grant = g;
          lag.push(performance.now() - start);
          const r = performance.now();
          assert.equal(
            (await q.renew({ lease: grant.lease, requestId: "renew" })).kind,
            "applied",
          );
          renewMs.push(performance.now() - r);
          assert.equal(
            (await q.complete({ lease: grant.lease, requestId: "done" })).kind,
            "applied",
          );
          completed++;
          activity[slot]++;
        }
      }),
    );
    const elapsedMs = performance.now() - start;
    return {
      workers,
      conflicts,
      workload,
      completed,
      claims,
      empty,
      elapsedMs,
      completedPerSec: (total * 1000) / elapsedMs,
      claimMs: quantiles(claimMs),
      renewMs: quantiles(renewMs),
      dispatchLagMs: quantiles(lag),
      workersWithJobs: activity.filter(Boolean).length,
      minJobs: Math.min(...activity),
      maxJobs: Math.max(...activity),
    };
  } finally {
    await owner.close();
    console.error(
      "measured",
      workers,
      workload,
      Math.round(performance.now() - realStart),
      "ms",
    );
  }
}
try {
  const results: Result[] = [];
  for (const w of [1, 8, 32, 128])
    for (const scenario of ["free-keys", "one-key", "filtered-prefix"])
      results.push(await measure(w, scenario));
  await mkdir("output", { recursive: true });
  await writeFile(
    "output/bench.json",
    JSON.stringify(
      {
        node: process.version,
        os: process.platform,
        cpu: cpus()[0].model,
        filesystemMagic: (await statfs(root)).type,
        clock:
          "logical for due/lease decisions, performance.now for measured durations",
        transport: "in-process",
        units:
          "latencies in ms [p50,p95,p99]; 128 completed runs per cell; zero handler work",
        results,
      },
      null,
      2,
    ) + "\n",
  );
} finally {
  await rm(root, { recursive: true, force: true });
}
