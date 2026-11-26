/** VERIFIED: The matrix kills both worker and authority and restarts the same files under the other runtime. Three browser engines then run the installed browser build, lose the owning page and restart on the same profile. */
import assert from "node:assert/strict";
import { spawn, execFileSync } from "node:child_process";
import {
  mkdtemp,
  copyFile,
  mkdir,
  readFile,
  rm,
  realpath,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import {
  chromium,
  firefox,
  webkit,
  type Page,
  type Request,
} from "playwright-core";
const dir = await mkdtemp(join(tmpdir(), "defuss-scheduler-consumer-"));
const { version: packageVersion } = JSON.parse(await readFile("package.json", "utf8")) as { version: string };
const archive = await realpath(`output/defuss-scheduler-${packageVersion}.tgz`);
const children = new Set<ReturnType<typeof spawn>>();
function run(runtime: string, mode: string, root: string, url = "") {
  const child = spawn(runtime, ["consumer.ts", mode, root, url], {
    cwd: dir,
    stdio: ["ignore", "pipe", "inherit"],
  });
  children.add(child);
  const lines = createInterface({ input: child.stdout })[
    Symbol.asyncIterator
  ]();
  const closed = new Promise<{ code: number | null; signal: string | null }>(
    (resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code, signal) => {
        children.delete(child);
        resolve({ code, signal });
      });
    },
  );
  return {
    child,
    closed,
    async report() {
      const line = await lines.next();
      assert.equal(line.done, false, mode + " missing report");
      return JSON.parse(line.value!);
    },
  };
}
async function round(a: string, b: string) {
  const root = join(dir, a);
  await mkdir(root);
  const host = run(a, "host", root);
  const { url } = await host.report();
  const worker = run(b, "first", root, url);
  const first = await worker.report();
  const peer = run(a, "contender", root, url);
  const conflict = await peer.report();
  assert.equal((await peer.closed).code, 0);
  worker.child.kill("SIGKILL");
  assert.equal((await worker.closed).signal, "SIGKILL");
  host.child.kill("SIGKILL");
  assert.equal((await host.closed).signal, "SIGKILL");
  const host2 = run(b, "reopen", root);
  const secondUrl = (await host2.report()).url;
  const retry = run(a, "replace", root, secondUrl);
  const second = await retry.report();
  assert.equal((await retry.closed).code, 0);
  host2.child.kill("SIGTERM");
  assert.equal((await host2.closed).code, 0);
  const inspect = run(a, "inspect", root);
  const final = await inspect.report();
  assert.equal((await inspect.closed).code, 0);
  assert.deepEqual(final, {
    named: { schedules: ["rollup"], runs: [true] },
    terminated: { schedules: 1, runs: 0 },
    state: "succeeded",
    attempts: 2,
    result: { changes: 1 },
    effectMatchesRun: true,
    history: ["succeeded", "leased", "retryWait", "leased", "ready"],
    scheduleStates: ["terminated", "paused", "active"],
  });
  const jobRun = run(b, "jobs", join(root, "jobs"));
  const jobs = await jobRun.report();
  assert.equal((await jobRun.closed).code, 0);
  assert.deepEqual(jobs, {
    state: "succeeded",
    result: { square: 49 },
    replayed: true,
    stopped: { schedules: 0, runs: 1 },
    causes: ["square-7 succeeded", "unclaimed terminated"],
  });
  return { firstOwner: a, restartedOwner: b, first, conflict, second, final, jobs };
}
const engines = { chromium, firefox, webkit };
type Scenarios = Record<string, () => Promise<unknown>>;
const scenario = (page: Page, name: string) =>
  page.evaluate(
    (n) => (window as unknown as { scenarios: Scenarios }).scenarios[n](),
    name,
  );
async function browserRound(name: keyof typeof engines) {
  const root = join(dir, name);
  await mkdir(root);
  const profile = join(dir, name + "-profile");
  const host = run("node", "host", root);
  const { url } = await host.report();
  // A persistent profile gives WebKit an origin private file system and lets the second launch find the first one's files.
  let context = await engines[name].launchPersistentContext(profile);
  // The store's archive codec sits behind a dynamic import that a scheduler never takes.
  const scripts = new Set<string>();
  const record = (request: Request) =>
    scripts.add(new URL(request.url()).pathname);
  context.on("request", record);
  try {
    const version = context.browser()?.version();
    const owner = await context.newPage();
    await owner.goto(url);
    const first = await scenario(owner, "first");
    const peer = await context.newPage();
    await peer.goto(url);
    const conflict = await scenario(peer, "busy");
    await owner.close();
    const takeover = await scenario(peer, "reopen");
    const dedicated = await scenario(peer, "dedicated");
    const remote = await scenario(peer, "remote");
    await context.close();
    context = await engines[name].launchPersistentContext(profile);
    context.on("request", record);
    const page = await context.newPage();
    await page.goto(url);
    const restart = await scenario(page, "restart");
    assert.ok(scripts.has("/consumer-browser.js"));
    assert.deepEqual(
      [...scripts].filter((path) => /compressor/.test(path)),
      [],
    );
    host.child.kill("SIGTERM");
    assert.equal((await host.closed).code, 0);
    const inspect = run("node", "inspect", root);
    const native = await inspect.report();
    assert.equal((await inspect.closed).code, 0);
    assert.deepEqual(native, {
      named: { schedules: ["rollup"], runs: [true] },
      terminated: { schedules: 1, runs: 0 },
      state: "succeeded",
      attempts: 1,
      result: { from: "browser" },
      effectMatchesRun: false,
      history: ["succeeded", "leased", "ready"],
      scheduleStates: ["terminated", "paused", "active"],
    });
    return {
      engine: name,
      version,
      first,
      conflict,
      takeover,
      dedicated,
      remote,
      restart,
      native,
    };
  } finally {
    await context.close().catch(() => {});
  }
}
const timer = setTimeout(() => {
  for (const c of children) c.kill("SIGKILL");
}, 150000);
try {
  await writeFile(
    join(dir, "package.json"),
    '{"name":"scheduler-consumer","private":true,"type":"module"}',
  );
  // defuss-multicore is the consumer's own choice of pool, as poolHandlers expects.
  execFileSync("bun", ["add", archive, "defuss-multicore@0.1.0"], {
    cwd: dir,
    stdio: "pipe",
    timeout: 90000,
  });
  await copyFile("scripts/pool-worker.mjs", join(dir, "pool-worker.mjs"));
  await copyFile("scripts/consumer-types.ts", join(dir, "consumer-types.ts"));
  const tsc = [
    await realpath("node_modules/typescript/bin/tsc"),
    "--strict",
    "--skipLibCheck",
    "--noEmit",
    "--target",
    "ES2024",
    "--module",
    "NodeNext",
  ];
  execFileSync("node", [...tsc, "consumer-types.ts"], {
    cwd: dir,
    stdio: "pipe",
  });
  await copyFile("scripts/consumer.ts", join(dir, "consumer.ts"));
  await copyFile("input/acceptance.json", join(dir, "acceptance.json"));
  await copyFile(
    "scripts/consumer-browser.ts",
    join(dir, "consumer-browser.ts"),
  );
  execFileSync("node", [...tsc, "--customConditions", "browser", "consumer-browser.ts"], {
    cwd: dir,
    stdio: "pipe",
  });
  execFileSync(
    "bun",
    [
      "build",
      "consumer-browser.ts",
      "--target",
      "browser",
      "--splitting",
      "--outdir",
      "web",
    ],
    { cwd: dir, stdio: "pipe" },
  );
  const bundle = await readFile(join(dir, "web/consumer-browser.js"), "utf8");
  assert.ok(
    !/@lickle|authority\.lock/.test(bundle),
    "browser bundle contains the native store edge",
  );
  await writeFile(
    join(dir, "web/index.html"),
    '<!doctype html><meta charset="utf-8"><title>scheduler consumer</title><script type="module" src="./consumer-browser.js"></script>',
  );
  const rounds = [await round("node", "bun"), await round("bun", "node")];
  const browsers = await Promise.all(
    (Object.keys(engines) as (keyof typeof engines)[]).map(browserRound),
  );
  const evidence = {
    platform: process.platform,
    arch: process.arch,
    node: process.version,
    bun: execFileSync("bun", ["--version"], { encoding: "utf8" }).trim(),
    icu: process.versions.icu,
    rounds,
    browsers,
  };
  await writeFile("output/e2e.json", JSON.stringify(evidence, null, 2) + "\n");
  console.log(JSON.stringify(evidence));
} finally {
  clearTimeout(timer);
  for (const c of children) c.kill("SIGKILL");
  await rm(dir, { recursive: true, force: true });
}
