/** VERIFIED: The public worker uses a complete lease identity and string fence counter. The packed TypeScript consumer checks the API and a rejected worker ID type. */
export type Json =
  null | boolean | number | string | Json[] | { [key: string]: Json };
export interface Scope {
  app: string;
  tenant: string;
  bucket: string;
}
export interface RetryPolicy {
  maxAttempts: number;
  baseMs: number;
  maxMs: number;
}
export interface Definition {
  id: string;
  /**
   * Your own name for what the schedule does. Unlike id it may repeat, so a schedule created after
   * a removed or terminated one carries the name on. Lists of schedules and runs filter by it.
   */
  ref?: string;
  cron: string;
  handler: string;
  payload?: Json;
  timezone?: string;
  capabilities?: string[];
  concurrencyKey?: string;
  parallel?: boolean;
  misfire?: "latest" | "catchUp" | "skip";
  graceMs?: number;
  retry?: Partial<RetryPolicy>;
}
export type Patch = Partial<
  Omit<Definition, "id" | "ref" | "concurrencyKey" | "parallel">
>;
export interface Change {
  revision: string;
  requestId: string;
}
export interface Schedule extends Required<
  Omit<Definition, "ref" | "concurrencyKey" | "retry">
> {
  ref?: string;
  concurrencyKey?: string;
  retry: RetryPolicy;
  revision: string;
  state: "active" | "paused" | "removed" | "terminated";
  /** The cause given to the hard stop that terminated the schedule; null in every other state. */
  terminationCause: string | null;
  nextDue: number | null;
  lastProcessed: number;
  lastSkipped: { from: number; through: number } | null;
  /** Epoch milliseconds of the first write and of the last change to the definition or state. */
  createdAt: number;
  updatedAt: number;
}
/** What a schedule's history records: its definition and state, without the moving calendar cursor. */
export type ScheduleRecord = Omit<
  Schedule,
  "nextDue" | "lastProcessed" | "lastSkipped"
>;
/** One state of a record and the interval in which it applied. A null value marks a deleted record. */
export interface Version<T> {
  from: number;
  until: number | null;
  value: T | null;
}
export interface HistoryOptions {
  limit?: number;
}
export interface Run {
  id: string;
  scheduleId: string;
  /** The ref of the schedule at the occurrence's admission. */
  ref?: string;
  scheduleRevision: string;
  scheduledAt: number;
  handler: string;
  payload: Json;
  capabilities: string[];
  payloadBytes: number;
  retry: RetryPolicy;
  attempt: number;
  availableAt: number;
  state: "ready" | "leased" | "retryWait" | "succeeded" | "dead" | "terminated";
  lastError: string | null;
  /** The cause given to the hard stop that terminated the run; null in every other state. */
  terminationCause: string | null;
  finishedAt: number | null;
  result: Json;
  /** Epoch milliseconds of the occurrence's admission and of its last status change. */
  createdAt: number;
  updatedAt: number;
}
export interface Claim {
  workerId: string;
  sessionId: string;
  slot: number;
  sequence: number;
  requestId: string;
  handlers: string[];
  capabilities?: string[];
  maxPayloadBytes?: number;
}
export interface Lease {
  runId: string;
  token: string;
  fence: string;
  workerId: string;
  sessionId: string;
  slot: number;
}
export interface Grant {
  kind: "granted";
  lease: Lease;
  run: Run;
  expiresAt: number;
  ttlMs: number;
}
export type ClaimResult =
  Grant | { kind: "empty" | "superseded"; retryAfterMs: number };
export interface Report {
  lease: Lease;
  requestId: string;
}
export type Outcome =
  | { kind: "applied" | "alreadyApplied"; expiresAt?: number; ttlMs?: number }
  | { kind: "stale" | "unknown" };
export interface WorkerPort {
  claim(input: Claim): Promise<ClaimResult>;
  renew(input: Report): Promise<Outcome>;
  complete(input: Report & { result?: Json }): Promise<Outcome>;
  fail(
    input: Report & { error: string; permanent?: boolean },
  ): Promise<Outcome>;
}
export interface PageOptions {
  cursor?: string;
  limit?: number;
  /** List only the schedules that carry this ref, or the runs of such schedules in scheduled order. */
  ref?: string;
}
/** What a hard stop applies to: one run, one schedule and its runs, every schedule carrying a ref, or the whole scope. */
export type TerminationTarget =
  { run: string } | { schedule: string } | { ref: string } | { all: true };
/** How many schedules and unfinished runs a hard stop terminated. */
export interface Terminated {
  schedules: number;
  runs: number;
}
export interface Page<T> {
  items: T[];
  cursor: string | null;
}
export interface Queue extends WorkerPort {
  create(def: Definition, options?: { requestId: string }): Promise<Schedule>;
  get(id: string): Promise<Schedule | null>;
  list(options?: PageOptions): Promise<Page<Schedule>>;
  update(id: string, patch: Patch, change: Change): Promise<Schedule>;
  pause(id: string, change: Change): Promise<Schedule>;
  resume(id: string, change: Change): Promise<Schedule>;
  remove(id: string, change: Change): Promise<Schedule>;
  getRun(id: string): Promise<Run | null>;
  listRuns(options?: PageOptions): Promise<Page<Run>>;
  /** The definitions and states a schedule went through, newest first. */
  scheduleHistory(
    id: string,
    options?: HistoryOptions,
  ): Promise<Version<ScheduleRecord>[]>;
  /** The statuses a run went through, newest first. Retention erases the history with the run. */
  runHistory(id: string, options?: HistoryOptions): Promise<Version<Run>[]>;
  /**
   * Stop work at once and record why. An edit waits until its calendar window has drained; this does not.
   * Terminated schedules admit nothing more, and unfinished runs end as `terminated`, leased ones included.
   */
  terminate(
    target: TerminationTarget,
    options: { cause: string },
  ): Promise<Terminated>;
}
export type SchedulerCode =
  | "INVALID_ARGUMENT"
  | "ALREADY_EXISTS"
  | "NOT_FOUND"
  | "REVISION_CONFLICT"
  | "BUSY"
  | "CLOSED"
  | "OWNER_BUSY"
  | "FORMAT"
  | "COMMIT_UNCERTAIN";
export class SchedulerError extends Error {
  readonly code: SchedulerCode;
  constructor(code: SchedulerCode, message: string) {
    super(message);
    this.name = "SchedulerError";
    this.code = code;
  }
}

/** Time spent waiting for the authority and inside one acknowledged store transaction. */
export interface TransactionMetric {
  queueMs: number;
  transactionMs: number;
  conflicts: number;
}
