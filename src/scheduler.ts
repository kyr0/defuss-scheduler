/** VERIFIED: The startup turn scans durable indexes before the first worker connection. Closing the handle drains admitted commands. */
import { openLedger, type Mounter } from "./store.js";
import {
  exposed,
  scopeKey,
  keys,
  listPage,
  loadSchedule,
  type StoredRun,
  type Pointer,
  type Policy,
} from "./model.js";
import { advance, create, modify } from "./schedules.js";
import { claim, collectSlot, expire, report, runId } from "./leases.js";
import { terminate } from "./termination.js";
import {
  type HistoryOptions,
  type Scope,
  type TransactionMetric,
  type Queue,
  type Run,
  type Schedule,
  type ScheduleRecord,
  SchedulerError,
} from "./types.js";
import { id, integer, json, object, timeKey } from "./validation.js";

function historyLimit(options: HistoryOptions): number {
  object(options, ["limit"]);
  return integer(options.limit ?? 50, 1, 500);
}

export interface SchedulerOptions {
  /** Node.js and Bun: a filesystem directory. Browser: the name of a store in the origin's private file system. */
  directory: string;
  leaseMs?: number;
  pollMs?: number;
  tickMs?: number;
  pageSize?: number;
  maxPending?: number;
  retentionMs?: number;
  autoTick?: boolean;
  clock?: () => number;
  onError?: (error: unknown) => void;
  onTransaction?: (metric: TransactionMetric) => void;
}
export interface Scheduler {
  scope(scope: Scope): Queue;
  tick(): Promise<void>;
  compact(): Promise<void>;
  close(): Promise<void>;
}
/** A single storage-backed authority. Startup recovers due work even before a worker connects. */
export async function start(
  options: SchedulerOptions,
  mount: Mounter,
): Promise<Scheduler> {
  const policy: Policy = {
    leaseMs: integer(options.leaseMs ?? 30_000, 50, 86400000),
    pollMs: integer(options.pollMs ?? 250, 1, 60000),
    maxPending: integer(
      options.maxPending ?? 100000,
      1,
      Number.MAX_SAFE_INTEGER,
    ),
    pageSize: integer(options.pageSize ?? 32, 1, 128),
    retentionMs: integer(options.retentionMs ?? 86400000, 1000, 31536000000),
  };
  const tickMs = integer(options.tickMs ?? 250, 1, 60000);
  const ledger = await openLedger(
    await mount(options.directory),
    options.clock,
    options.onTransaction,
  );
  let dueCursor: string | undefined,
    timer: ReturnType<typeof setTimeout> | undefined;
  let closed = false,
    ticking: Promise<void> | undefined;
  /**
   * Whether the last turn filled its retention page. The next turn then follows at once: one page per
   * tickMs would cap retention at a fixed rate, and a higher rate of finished runs would grow the store.
   * A turn deletes or postpones every entry it reads, so the pages of a backlog end. The due and expiry
   * pages do not count, because a saturated schedule stays due and would repeat the turn without progress.
   */
  let backlog = false;
  async function maintain() {
    backlog = false;
    const now = ledger.wallNow(),
      leaseTime = ledger.leaseNow();
    const { due, ended, full } = await ledger.write(async (tx) => {
      const expired = await tx.rows<Pointer>(
        "e/",
        policy.pageSize,
        undefined,
        "e/" + timeKey(leaseTime) + "/\uffff",
      );
      for (const { value: p } of expired)
        await expire(tx, p.scope, p.id, ledger, policy);
      const gc = await tx.rows<string[]>(
        "z/",
        policy.pageSize,
        undefined,
        "z/" + timeKey(now) + "/\uffff",
      );
      const ended = [];
      for (const row of gc) {
        for (const key of row.value)
          if (key.startsWith("w/")) await collectSlot(tx, key, now, policy);
          else tx.del(key);
        // A run's entry stays until its history is erased, so a crash before that repeats the erasure.
        if (row.value[0].startsWith("r/")) ended.push(row);
        else tx.del(row.key);
      }
      const due = await tx.rows<Pointer>(
        "d/",
        policy.pageSize,
        dueCursor,
        "d/" + timeKey(now) + "/\uffff",
      );
      return { due, ended, full: gc.length === policy.pageSize };
    });
    if (ended.length) {
      await ledger.erase(ended.map((row) => row.value[0]));
      await ledger.write(async (tx) => {
        for (const row of ended) tx.del(row.key);
      });
    }
    for (const { value: p } of due)
      await ledger.write((tx) => advance(tx, p.scope, p.id, now, policy));
    dueCursor = due.length === policy.pageSize ? due.at(-1)!.key : undefined;
    backlog = full;
  }
  function tick(): Promise<void> {
    if (closed)
      return Promise.reject(new SchedulerError("CLOSED", "Scheduler closed"));
    return (ticking ??= maintain().finally(() => {
      ticking = undefined;
    }));
  }
  function arm() {
    if (closed || options.autoTick === false) return;
    timer = setTimeout(
      async () => {
        try {
          await tick();
        } catch (error) {
          if (!closed) (options.onError ?? console.error)(error);
        } finally {
          arm();
        }
      },
      backlog ? 0 : tickMs,
    );
    // A native timer must not keep an otherwise finished process alive; a browser timer is a plain number.
    (timer as { unref?: () => void }).unref?.();
  }
  try {
    await tick();
    arm();
  } catch (error) {
    await ledger.close();
    throw error;
  }
  let closePromise: Promise<void> | undefined;
  // Capture caller-owned values before admission to the asynchronous queue.
  const capture = <T>(v: T): T => {
    if (closed) throw new SchedulerError("CLOSED", "Scheduler closed");
    return json(v, 320000) as T;
  };
  return {
    tick,
    compact: ledger.compact,
    close() {
      closed = true;
      clearTimeout(timer);
      return (closePromise ??= Promise.resolve(ticking)
        .catch(() => {})
        .then(() => ledger.close()));
    },
    scope(input) {
      const ns = scopeKey(input);
      return {
        create: async (def, opts = { requestId: crypto.randomUUID() }) =>
          create(ledger, ns, capture(def), capture(opts), policy),
        get: async (name) => {
          const k = keys.schedule(ns, id(capture(name)));
          return ledger.write(
            async (tx) => (await loadSchedule(tx, k)) ?? null,
          );
        },
        list: async (args = {}) => {
          const opts = capture(args);
          return ledger.write(async (tx) => {
            const page = await listPage<ScheduleRecord | string>(
              tx,
              opts,
              "s/" + ns + "/",
              (ref) => keys.scheduleRefs(ns, ref),
            );
            const items: Schedule[] = [];
            for (const { key, value } of page.items)
              items.push(
                (await loadSchedule(
                  tx,
                  typeof value === "string" ? value : key,
                ))!,
              );
            return { items, cursor: page.cursor };
          });
        },
        pause: async (name, c) =>
          modify(ledger, ns, name, "pause", {}, capture(c), policy),
        resume: async (name, c) =>
          modify(ledger, ns, name, "resume", {}, capture(c), policy),
        remove: async (name, c) =>
          modify(ledger, ns, name, "remove", {}, capture(c), policy),
        update: async (name, p, c) =>
          modify(ledger, ns, name, "update", capture(p), capture(c), policy),
        getRun: async (rid) => {
          const k = keys.run(ns, runId(capture(rid)));
          return ledger.write(async (tx) => {
            const run = await tx.get<StoredRun>(k);
            return run ? exposed(run) : null;
          });
        },
        listRuns: async (args = {}) => {
          const opts = capture(args);
          return ledger.write(async (tx) => {
            const page = await listPage<StoredRun | string>(
              tx,
              opts,
              "r/" + ns + "/",
              (ref) => keys.runRefs(ns, ref),
            );
            const items: Run[] = [];
            for (const { value } of page.items)
              items.push(
                exposed(
                  typeof value === "string"
                    ? (await tx.get<StoredRun>(value))!
                    : value,
                ),
              );
            return { items, cursor: page.cursor };
          });
        },
        scheduleHistory: async (name, args = {}) =>
          ledger.history<ScheduleRecord>(
            keys.schedule(ns, id(capture(name))),
            historyLimit(capture(args)),
          ),
        runHistory: async (rid, args = {}) =>
          (
            await ledger.history<StoredRun>(
              keys.run(ns, runId(capture(rid))),
              historyLimit(capture(args)),
            )
          ).map((v) => ({ ...v, value: v.value && exposed(v.value) })),
        terminate: async (target, options) =>
          terminate(ledger, ns, capture(target), capture(options), policy),
        claim: async (input) => claim(ledger, ns, capture(input), policy),
        renew: async (input) =>
          report(ledger, ns, "renew", capture(input), policy),
        fail: async (input) =>
          report(ledger, ns, "fail", capture(input), policy),
        complete: async (input) =>
          report(ledger, ns, "complete", capture(input), policy),
      };
    },
  };
}
