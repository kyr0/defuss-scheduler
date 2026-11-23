/** VERIFIED: A hard stop marks its schedules first and then ends their unfinished runs a page per transaction. The store tests stop a catch-up backlog that an edit answers with BUSY. */
import { keys, type Policy, type StoredRun } from "./model.js";
import { runId, terminateRun } from "./leases.js";
import { halt } from "./schedules.js";
import type { Ledger } from "./store.js";
import {
  type ScheduleRecord,
  type Terminated,
  type TerminationTarget,
  SchedulerError,
} from "./types.js";
import { id, invalid, object } from "./validation.js";

/**
 * Selective targets name one run or one schedule; `ref` and `all` stop collectively. Schedules stop before
 * any run does, so nothing new is admitted while the runs end. Each page is its own transaction, which lets
 * worker reports and other commands pass between pages of a large backlog.
 */
export async function terminate(
  ledger: Ledger,
  ns: string,
  target: TerminationTarget,
  options: { cause: string },
  policy: Policy,
): Promise<Terminated> {
  object(target, ["run", "schedule", "ref", "all"]);
  if (Object.keys(target).length !== 1)
    invalid("Name exactly one of run, schedule, ref or all");
  object(options, ["cause"]);
  const { cause } = options;
  if (typeof cause !== "string" || !cause || cause.length > 4096)
    invalid("A termination needs a cause of 1 to 4096 characters");
  const total: Terminated = { schedules: 0, runs: 0 };

  if ("run" in target) {
    const key = keys.run(ns, runId(target.run));
    total.runs = await ledger.write(async (tx) => {
      const run = await tx.get<StoredRun>(key);
      if (!run) throw new SchedulerError("NOT_FOUND", "Run absent");
      if (run.public.finishedAt !== null) return 0;
      await terminateRun(tx, run, ledger.wallNow(), policy, cause);
      return 1;
    });
    return total;
  }

  /** Stops the schedules under a prefix and returns their IDs. Index rows hold a schedule's key as their value. */
  async function stop(prefix: string): Promise<string[]> {
    const ids: string[] = [];
    for (let after: string | undefined, more = true; more; ) {
      const page = await ledger.write(async (tx) => {
        const rows = await tx.rows<ScheduleRecord | string>(
          prefix,
          policy.pageSize,
          after,
        );
        const halted = [];
        for (const { key, value } of rows)
          halted.push(
            await halt(tx, ns, typeof value === "string" ? value : key, cause),
          );
        return { halted, last: rows.at(-1)?.key };
      });
      for (const { id: name, stopped } of page.halted) {
        ids.push(name);
        if (stopped) total.schedules++;
      }
      after = page.last;
      more = page.halted.length === policy.pageSize;
    }
    return ids;
  }
  /** Ends the unfinished runs under a prefix. Each ended run leaves the index, so every page starts at its head. */
  async function sweep(prefix: string) {
    for (let ended = policy.pageSize; ended === policy.pageSize; ) {
      ended = await ledger.write(async (tx) => {
        const rows = await tx.rows<string>(prefix, policy.pageSize);
        for (const { value } of rows)
          await terminateRun(
            tx,
            (await tx.get<StoredRun>(value))!,
            ledger.wallNow(),
            policy,
            cause,
          );
        return rows.length;
      });
      total.runs += ended;
    }
  }

  if ("all" in target) {
    if (target.all !== true) invalid("Expected all: true");
    await stop("s/" + ns + "/");
    await sweep(keys.open(ns));
    return total;
  }
  let ids: string[];
  if ("schedule" in target) {
    const key = keys.schedule(ns, id(target.schedule));
    const { stopped } = await ledger.write((tx) => halt(tx, ns, key, cause));
    if (stopped) total.schedules = 1;
    ids = [target.schedule];
  } else ids = await stop(keys.scheduleRefs(ns, id(target.ref)));
  for (const name of ids) await sweep(keys.open(ns, name));
  return total;
}
