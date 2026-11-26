/** VERIFIED: Tuple-encoded scopes separate worker groups and paginated records. The store tests reuse one schedule name across tenants. */
import { calendar } from "./calendar.js";
import {
  decode,
  encode,
  id,
  integer,
  invalid,
  json,
  names,
  object,
  timeKey,
} from "./validation.js";
import {
  type Definition,
  type RetryPolicy,
  type Schedule,
  type ScheduleRecord,
  type Scope,
  type Run,
  type Lease,
  type PageOptions,
  type Page,
} from "./types.js";
import type { Records } from "./store.js";

export interface Policy {
  leaseMs: number;
  pageSize: number;
  maxPending: number;
  retentionMs: number;
  pollMs: number;
}
/** A schedule in memory before the store has stamped it. */
export type Draft = Omit<Schedule, "createdAt" | "updatedAt">;
export interface Cursor {
  nextDue: number | null;
  lastProcessed: number;
  lastSkipped: { from: number; through: number } | null;
}
/** The run's document changes with its status alone. The moving lease deadline lives in its group record. */
export interface StoredRun {
  public: Omit<Run, "createdAt" | "updatedAt">;
  group: string;
  lease?: Lease;
  scope: string;
  createdAt?: number;
  updatedAt?: number;
}
/** Runs a 0.2.0 store wrote lack the job fields, and read as the occurrences they are. */
export function exposed(run: StoredRun): Run {
  return {
    ...run.public,
    jobId: run.public.jobId ?? null,
    deadline: run.public.deadline ?? null,
    on: run.public.on ?? null,
    cause: run.public.cause ?? null,
    createdAt: run.createdAt!,
    updatedAt: run.updatedAt!,
  };
}
/** A schedule's definition and state are one tracked document; its calendar cursor moves with every occurrence and stays out of the history. */
export async function loadSchedule(
  tx: Records,
  key: string,
): Promise<Schedule | undefined> {
  const record = await tx.get<ScheduleRecord>(key);
  if (!record) return undefined;
  return { ...record, ...(await tx.get<Cursor>("c/" + key))! };
}
export interface ReadyEntry {
  id: string;
  group: string;
  handler: string;
  capabilities: string[];
  payloadBytes: number;
}
export function enqueue(tx: Records, run: StoredRun) {
  const { id, handler, capabilities, payloadBytes } = run.public;
  tx.put(keys.ready(run), {
    id,
    group: run.group,
    handler,
    capabilities,
    payloadBytes,
  } satisfies ReadyEntry);
  tx.put(keys.byHandler(run), run.scope);
}
/** Removes a run's two ready entries, the scope's queue entry and the authority-wide one by handler. */
export function dequeue(tx: Records, run: StoredRun) {
  tx.del(keys.ready(run));
  tx.del(keys.byHandler(run));
}
export interface Pointer {
  scope: string;
  id: string;
}
export const keys = {
  schedule: (s: string, name: string) => "s/" + s + "/" + encode(name),
  run: (s: string, rid: string) => "r/" + s + "/" + rid,
  due: (at: number, s: string, name: string) =>
    "d/" + timeKey(at) + "/" + s + "/" + encode(name),
  ready: (run: StoredRun) =>
    "q/" +
    run.scope +
    "/" +
    timeKey(run.public.availableAt) +
    "/" +
    run.public.id,
  /** Ready runs of every scope by handler and availability, so `Scheduler.ready` finds the scopes with work. */
  byHandler: (run: StoredRun) =>
    handlerPrefix(run.public.handler) +
    timeKey(run.public.availableAt) +
    "/" +
    run.scope +
    "/" +
    run.public.id,
  expiry: (at: number, s: string, rid: string) =>
    "e/" + timeKey(at) + "/" + s + "/" + rid,
  slot: (s: string, l: Pick<Lease, "workerId" | "sessionId" | "slot">) =>
    "w/" + s + "/" + encode([l.workerId, l.sessionId, l.slot]),
  /** A parallel run is alone in its group, so the group's record ends with the run. */
  ownGroup: (s: string, rid: string) => "g/" + s + "/" + encode(["run", rid]),
  /** Unfinished runs by schedule, so a hard stop finds them without scanning the scope's runs. */
  open: (s: string, name?: string) =>
    "u/" + s + "/" + (name === undefined ? "" : encode(name) + "/"),
  /**
   * Unfinished jobs, inside the scope's open prefix so a stop of the whole scope reaches them. An encoded array never
   * equals an encoded schedule name, which is a string.
   */
  openJobs: (s: string) => "u/" + s + "/" + encode(["job"]) + "/",
  /** Unfinished follow-ups, outside that prefix: a hard stop never cancels the notice of an ending. */
  openFollowUps: (s: string) => "uf/" + s + "/",
  /** Unfinished jobs by deadline, so maintenance ends them when it passes. */
  deadline: (at: number, s: string, rid: string) =>
    "x/" + timeKey(at) + "/" + s + "/" + rid,
  /** Schedules and runs by ref. Each entry's value is the key of the record it points to. */
  scheduleRefs: (s: string, ref: string) => "is/" + s + "/" + encode(ref) + "/",
  runRefs: (s: string, ref: string) => "ir/" + s + "/" + encode(ref) + "/",
};
/** A job's run ID derives from its ID, so `getJob` finds it without an index. */
export const jobRunId = (job: string) => encode(["job", job]);
export const openKey = (run: StoredRun) =>
  (run.public.scheduleId != null
    ? keys.open(run.scope, run.public.scheduleId)
    : run.public.jobId != null
      ? keys.openJobs(run.scope)
      : keys.openFollowUps(run.scope)) + run.public.id;
/** Runs of one ref sort by their scheduled time, across the schedules that carried it. */
export const runRefKey = (run: StoredRun) =>
  keys.runRefs(run.scope, run.public.ref!) +
  timeKey(run.public.scheduledAt) +
  "/" +
  run.public.id;
export const handlerPrefix = (handler: string) => "h/" + encode(handler) + "/";
export function scopeKey(scope: Scope): string {
  object(scope, ["app", "tenant", "bucket"]);
  return encode([id(scope.app), id(scope.tenant), id(scope.bucket)]);
}
export function schedule(input: Definition, now: number): Draft {
  object(input, [
    "id",
    "ref",
    "handler",
    "payload",
    "cron",
    "timezone",
    "capabilities",
    "misfire",
    "graceMs",
    "retry",
    "concurrencyKey",
    "parallel",
  ]);
  const timezone = input.timezone === undefined ? "UTC" : input.timezone;
  const expr = calendar(input.cron, timezone);
  const next = expr.nextRun(new Date(now));
  if (!next) invalid("Expression has no future match");
  const misfire = input.misfire ?? "latest";
  if (!["latest", "catchUp", "skip"].includes(misfire))
    invalid("Unknown misfire policy");
  if (input.parallel !== undefined && typeof input.parallel !== "boolean")
    invalid("Invalid parallel option");
  if (input.concurrencyKey !== undefined && input.parallel)
    invalid("Choose either a shared or per-run concurrency key");
  return {
    id: id(input.id),
    ...(input.ref !== undefined && { ref: id(input.ref) }),
    handler: id(input.handler),
    cron: input.cron.trim(),
    timezone,
    payload: json(input.payload === undefined ? null : input.payload),
    capabilities: names(input.capabilities ?? []),
    misfire,
    graceMs: integer(input.graceMs ?? 60_000, 0, Number.MAX_SAFE_INTEGER),
    retry: retryPolicy(input.retry),
    parallel: input.parallel ?? false,
    ...(input.concurrencyKey !== undefined && {
      concurrencyKey: id(input.concurrencyKey),
    }),
    revision: crypto.randomUUID(),
    state: "active",
    terminationCause: null,
    nextDue: next.getTime(),
    lastProcessed: now,
    lastSkipped: null,
  };
}
/** 3 attempts, a 500 ms base and a 30 s cap unless the caller sets them; schedules, jobs and follow-ups share it. */
export function retryPolicy(retry: Partial<RetryPolicy> | undefined): RetryPolicy {
  if (retry !== undefined) object(retry, ["maxAttempts", "baseMs", "maxMs"]);
  const baseMs = integer(retry?.baseMs ?? 500, 0, 86_400_000);
  return {
    baseMs,
    maxMs: integer(retry?.maxMs ?? Math.max(baseMs, 30_000), baseMs, 86_400_000),
    maxAttempts: integer(retry?.maxAttempts ?? 3, 1, 10_000),
  };
}
export function definition(s: Schedule): Definition {
  const {
    revision: _r,
    state: _s,
    terminationCause: _t,
    nextDue: _n,
    lastProcessed: _p,
    lastSkipped: _l,
    createdAt: _c,
    updatedAt: _u,
    ...d
  } = s;
  return d;
}
export async function count(tx: Records, scope: string, delta = 0) {
  const key = "n/" + scope;
  const result = ((await tx.get<number>(key)) ?? 0) + delta;
  if (delta) tx.put(key, result);
  return result;
}
/** After `at`, maintenance deletes these keys in one transaction. */
export function collectLater(tx: Records, at: number, ...targets: string[]) {
  tx.put("z/" + timeKey(at) + "/" + targets[0], targets);
}
/**
 * One page of rows: the scope's records, or with a ref the entries of that ref's index,
 * whose values are the keys of the records.
 */
export async function listPage<V>(
  tx: Records,
  args: PageOptions,
  records: string,
  refs: (ref: string) => string,
): Promise<Page<{ key: string; value: V }>> {
  object(args, ["limit", "cursor", "ref"]);
  const prefix = args.ref === undefined ? records : refs(id(args.ref));
  const limit = integer(args.limit ?? 50, 1, 500);
  let after: string | undefined;
  if (args.cursor !== undefined) {
    if (typeof args.cursor !== "string" || args.cursor.length > 5000)
      invalid("Invalid cursor");
    try {
      after = decode(args.cursor) as string;
    } catch {
      invalid("Invalid cursor");
    }
    if (
      typeof after !== "string" ||
      !after.startsWith(prefix) ||
      after.includes("\0")
    )
      invalid("Cursor outside scope");
  }
  const rows = await tx.rows<V>(prefix, limit + 1, after);
  return {
    items: rows.slice(0, limit),
    cursor: rows.length > limit ? encode(rows[limit - 1].key) : null,
  };
}
