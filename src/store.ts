/** VERIFIED: The authority holds its platform's exclusive lock across its store lifetime and reopens the committed head before replay after an uncertain publication. Schedule and run documents keep their history and timestamps in defuss-db, and a collected run is erased with its history. */
import {
  openDatabase,
  type Database,
  type DatabaseBackend,
  type DatabaseTransaction,
} from "defuss-db";
import { SchedulerError, type TransactionMetric } from "./types.js";

/**
 * The platform edge: exclusive ownership of one store, held from mount to release. Native files and
 * browser origin storage differ only here, so the ledger and every decision above it run unchanged.
 */
export interface Mount {
  /** A fresh storage backend; called again to reopen the committed head. */
  connect(): DatabaseBackend | Promise<DatabaseBackend>;
  release(): Promise<void>;
}
/** Rejects OWNER_BUSY while another authority holds the same directory. */
export type Mounter = (directory: string) => Promise<Mount>;

/**
 * Schedules and runs are the records an operator follows over time, so their collections track history and
 * timestamps. Everything else is coordination state that changes with every poll and heartbeat: a history of
 * it would grow without bound and say nothing about a job.
 */
const TRACKED = ["schedules", "runs"] as const;
const COLLECTIONS = [...TRACKED, "ledger"] as const;
const collectionOf = (key: string) =>
  key.startsWith("s/") ? "schedules" : key.startsWith("r/") ? "runs" : "ledger";
const tracked = (key: string) => collectionOf(key) !== "ledger";

interface Stored {
  id: string;
  value: unknown;
  _createdAt?: Date;
  _updatedAt?: Date;
}
/** A document's value, with the collection's timestamps as epoch milliseconds on tracked records. */
function unwrap<T>(doc: Stored): T {
  if (!doc._createdAt || !doc._updatedAt) return doc.value as T;
  return {
    ...(doc.value as object),
    createdAt: doc._createdAt.getTime(),
    updatedAt: doc._updatedAt.getTime(),
  } as T;
}
/** One state of a tracked record and the interval in which it applied. */
export interface Segment<T> {
  from: number;
  until: number | null;
  value: T | null;
}

export class Records {
  changed = false;
  /** Callers stage writes without awaiting; the database wants them one at a time, in order. */
  private staged: (() => Promise<unknown>)[] = [];
  constructor(private readonly tx: DatabaseTransaction) {}
  private in(key: string) {
    return this.tx.collection<Stored>(collectionOf(key));
  }
  /** Reads see every earlier write of their transaction. */
  async flush() {
    const writes = this.staged;
    this.staged = [];
    for (const write of writes) await write();
  }
  async get<T>(key: string): Promise<T | undefined> {
    await this.flush();
    const doc = await this.in(key).get(key);
    return doc && unwrap<T>(doc);
  }
  put(key: string, value: unknown) {
    if (tracked(key)) {
      // The database owns both stamps; a value read earlier carries copies of them.
      const { createdAt: _c, updatedAt: _u, ...rest } = value as Record<
        string,
        unknown
      >;
      value = rest;
    }
    // Capture the value as it is at this call; the caller may keep changing its object.
    const doc = { id: key, value: structuredClone(value) };
    this.staged.push(() => this.in(key).put(doc));
    this.changed = true;
  }
  del(key: string) {
    this.staged.push(() => this.in(key).delete(key));
    this.changed = true;
  }
  async rows<T>(
    prefix: string,
    limit: number,
    after?: string,
    lt = prefix + "￿",
  ) {
    await this.flush();
    const page = await this.in(prefix).pageByIndex("key", {
      ...(after ? { gt: after } : { gte: prefix }),
      lt,
      limit,
      // A page of runs can carry 500 payloads and results of 128 KiB each.
      maxOutputBytes: 2 ** 30,
    });
    return page.rows.map((doc) => ({ key: doc.id, value: unwrap<T>(doc) }));
  }
}
async function connect(guard: Mount, create: boolean) {
  const db = await openDatabase(await guard.connect(), {
    name: "defuss_scheduler",
    create,
    // Every read runs inside a write transaction, which bypasses the result cache.
    queryCache: false,
  });
  try {
    const known = await db.collections();
    if (known.some((name) => !COLLECTIONS.includes(name as never)))
      throw new SchedulerError("FORMAT", "Unsupported scheduler store");
    for (const name of TRACKED)
      await db.ensureCollection(name, {
        $trackHistory: true,
        $timestamps: true,
      });
    // Document IDs are the ledger keys, so one ordered index serves every prefix scan.
    for (const name of COLLECTIONS)
      await db
        .collection(name)
        .createIndex({ name: "key", fields: ["id"], order: ["asc"] });
    return db;
  } catch (error) {
    await db.close();
    throw error;
  }
}
export async function openLedger(
  guard: Mount,
  clock?: () => number,
  measure?: (metric: TransactionMetric) => void,
) {
  let db: Database | undefined;
  let anchor = Date.now();
  const origin = performance.now();
  try {
    db = await connect(guard, true);
    await db.transaction(async (tx) => {
      const records = new Records(tx);
      const marker = await records.get<number>("!version");
      if (marker !== undefined && marker !== 1)
        throw new SchedulerError("FORMAT", "Unsupported scheduler store");
      if (marker === undefined) {
        for (const name of COLLECTIONS)
          if (
            (await tx.collection(name).pageIdsByIndex("key", { limit: 1 })).rows
              .length
          )
            throw new SchedulerError("FORMAT", "Unmarked nonempty store");
        records.put("!version", 1);
      }
      anchor = Math.max(
        anchor,
        (await records.get<number>("!leaseTime")) ?? 0,
      );
      await records.flush();
    });
  } catch (e) {
    try {
      await db?.close();
    } finally {
      await guard.release();
    }
    throw e;
  }
  let live: Database = db;
  const leaseNow =
    clock ?? (() => Math.floor(anchor + performance.now() - origin));
  const wallNow = clock ?? Date.now;
  let tail: Promise<unknown> = Promise.resolve();
  let closing: Promise<void> | undefined;
  let broken = false;
  /** Every command runs alone, in call order. */
  function enqueue<T>(job: () => Promise<T>): Promise<T> {
    if (closing)
      return Promise.reject(new SchedulerError("CLOSED", "Scheduler closed"));
    const result = tail.then(() => {
      if (broken)
        throw new SchedulerError(
          "COMMIT_UNCERTAIN",
          "Reopen the authority to recover the store",
        );
      return job();
    });
    tail = result.catch(() => {});
    return result;
  }
  /** One store operation, repeated after a conflict or an uncertain publication. */
  function attempt<T>(operation: (conflicts: number) => Promise<T>) {
    return enqueue(async () => {
      for (let i = 0, conflicts = 0; ; i++) {
        try {
          return await operation(conflicts);
        } catch (error) {
          const code = (error as { code?: string })?.code;
          if (code === "CONFLICT" && i < 3) {
            conflicts++;
            continue;
          }
          if (code !== "COMMIT_UNCERTAIN") throw error;
          // Keep the lifetime guard. Reopen the committed head before replaying a
          // receipt-protected mutation or an idempotent maintenance operation.
          broken = true;
          await live.close();
          live = await connect(guard, false);
          broken = false;
          if (i >= 2) throw error;
        }
      }
    });
  }
  function write<T>(fn: (records: Records) => Promise<T>): Promise<T> {
    const queuedAt = measure ? performance.now() : 0;
    let startedAt = 0;
    return attempt(async (conflicts) => {
      if (measure) startedAt ||= performance.now();
      const { result } = await live.transaction(async (tx) => {
        const records = new Records(tx);
        const result = await fn(records);
        if (records.changed) records.put("!leaseTime", leaseNow());
        await records.flush();
        return result;
      });
      if (measure) {
        try {
          measure({
            queueMs: startedAt - queuedAt,
            transactionMs: performance.now() - startedAt,
            conflicts,
          });
        } catch {
          /* An observer cannot change a committed command's result. */
        }
      }
      return result;
    });
  }
  return {
    write,
    leaseNow,
    wallNow,
    /**
     * Removes deleted runs with every version of their history. The database commits it in batches, so one
     * run with thousands of attempts cannot exceed a transaction. A repeat skips what is gone.
     */
    erase: (runs: readonly string[]) =>
      attempt(() => live.collection<Stored>("runs").pruneDeleted({ ids: runs })),
    /** The database compacts itself as its log grows; this forces one pass at a time the caller chooses. */
    compact: () =>
      enqueue(async () => {
        await live.compact({ mode: "exclusive" });
        await live.collect();
      }),
    /**
     * The states a tracked record went through, newest first. The audit lists every recorded change;
     * the effective timeline would drop a status that was replaced within its own millisecond.
     */
    history: <T>(key: string, limit: number) =>
      enqueue(async (): Promise<Segment<T>[]> => {
        const changes = await live
          .collection<Stored>(collectionOf(key))
          .audit(key, { limit });
        return changes.map((change, i) => ({
          from: change.from.getTime(),
          until: i ? changes[i - 1].from.getTime() : null,
          value: change.document ? unwrap<T>(change.document) : null,
        }));
      }),
    close() {
      return (closing ??= tail.then(async () => {
        try {
          await live.close();
        } finally {
          await guard.release();
        }
      }));
    },
  };
}
export type Ledger = Awaited<ReturnType<typeof openLedger>>;
