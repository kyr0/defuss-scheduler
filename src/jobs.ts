/** VERIFIED: tests/jobs.test.ts. One-off jobs: a run without a schedule, admitted at once, with an optional deadline and
 * follow-ups by ending. Why a run and not a schedule that fires once: workers, leases, retries, refs, histories and
 * retention then treat a job like any occurrence, and nothing waits for a calendar. */
import {
  collectLater,
  count,
  enqueue,
  exposed,
  jobRunId,
  keys,
  openKey,
  retryPolicy,
  runRefKey,
  type Policy,
  type StoredRun,
} from "./model.js";
import type { Ledger, Records } from "./store.js";
import { type EndState, type FollowUp, type Job, type Run, SchedulerError } from "./types.js";
import { byteLength, encode, fingerprint, id, integer, json, names, object } from "./validation.js";

const ENDINGS: readonly EndState[] = ["succeeded", "dead", "timedOut", "terminated"];
type On = NonNullable<Run["on"]>;

function followUps(on: unknown): On | null {
  if (on === undefined) return null;
  object(on, ENDINGS);
  const result: On = {};
  for (const [state, value] of Object.entries(on) as [EndState, FollowUp][]) {
    object(value, ["handler", "payload", "capabilities", "retry"]);
    result[state] = {
      handler: id(value.handler),
      payload: json(value.payload === undefined ? null : value.payload),
      capabilities: names(value.capabilities ?? []),
      retry: retryPolicy(value.retry),
    };
  }
  return result;
}

/** A follow-up of a run's ending, written in the transaction that ends the run. It counts toward `maxPending` but is
 * admitted even at the limit, so an ending never fails for lack of room. */
export async function admitFollowUp(tx: Records, run: StoredRun, wall: number) {
  const state = run.public.state as EndState;
  const next = run.public.on?.[state];
  if (!next || run.public.jobId == null) return;
  const rid = encode(["then", run.public.id, state]);
  const followUp: StoredRun = {
    scope: run.scope,
    group: keys.ownGroup(run.scope, rid),
    public: {
      id: rid,
      scheduleId: null,
      jobId: null,
      ...(run.public.ref !== undefined && { ref: run.public.ref }),
      scheduleRevision: null,
      scheduledAt: wall,
      handler: next.handler,
      payload: next.payload,
      capabilities: next.capabilities,
      payloadBytes: byteLength(JSON.stringify(next.payload)),
      retry: next.retry,
      attempt: 0,
      availableAt: wall,
      state: "ready",
      lastError: null,
      terminationCause: null,
      finishedAt: null,
      result: null,
      deadline: null,
      on: null,
      cause: { job: run.public.jobId, run: run.public.id, state },
    },
  };
  tx.put(keys.run(run.scope, rid), followUp);
  enqueue(tx, followUp);
  tx.put(openKey(followUp), keys.run(run.scope, rid));
  if (followUp.public.ref !== undefined) tx.put(runRefKey(followUp), keys.run(run.scope, rid));
  await count(tx, run.scope, 1);
}

interface Receipt {
  hash: string;
  value: Run;
}

export function submit(ledger: Ledger, ns: string, input: Job, options: { requestId: string }, policy: Policy) {
  object(options, ["requestId"]);
  const rk = "m/" + ns + "/" + encode(id(options.requestId));
  const hashing = fingerprint(["submit", input]);
  return ledger.write(async (tx) => {
    const hash = await hashing;
    const known = await tx.get<Receipt>(rk);
    if (known) {
      if (known.hash !== hash) throw new SchedulerError("REVISION_CONFLICT", "Request ID reused with different arguments");
      return known.value;
    }
    object(input, ["id", "ref", "handler", "payload", "capabilities", "concurrencyKey", "availableAt", "timeoutMs", "retry", "on"]);
    const now = ledger.wallNow();
    const jobId = id(input.id);
    const rid = jobRunId(jobId);
    const key = keys.run(ns, rid);
    if (await tx.get(key)) throw new SchedulerError("ALREADY_EXISTS", "A job with this ID exists in the scope");
    if ((await count(tx, ns)) >= policy.maxPending)
      throw new SchedulerError("BUSY", "The scope holds maxPending unfinished runs; retry after some end");
    const payload = json(input.payload === undefined ? null : input.payload);
    const availableAt = integer(input.availableAt ?? now, 0, Number.MAX_SAFE_INTEGER);
    const timeoutMs = input.timeoutMs === undefined ? undefined : integer(input.timeoutMs, 1, 31_536_000_000);
    const on = followUps(input.on);
    const run: StoredRun = {
      scope: ns,
      group:
        input.concurrencyKey === undefined
          ? keys.ownGroup(ns, rid)
          : "g/" + ns + "/" + encode(["named", id(input.concurrencyKey)]),
      public: {
        id: rid,
        scheduleId: null,
        jobId,
        ...(input.ref !== undefined && { ref: id(input.ref) }),
        scheduleRevision: null,
        scheduledAt: availableAt,
        handler: id(input.handler),
        payload,
        capabilities: names(input.capabilities ?? []),
        payloadBytes: byteLength(JSON.stringify(payload)),
        retry: retryPolicy(input.retry),
        attempt: 0,
        availableAt,
        state: "ready",
        lastError: null,
        terminationCause: null,
        finishedAt: null,
        result: null,
        deadline: timeoutMs === undefined ? null : now + timeoutMs,
        on,
        cause: null,
      },
    };
    tx.put(key, run);
    enqueue(tx, run);
    tx.put(openKey(run), key);
    if (run.public.ref !== undefined) tx.put(runRefKey(run), key);
    if (run.public.deadline !== null) tx.put(keys.deadline(run.public.deadline, ns, rid), { scope: ns, id: rid });
    await count(tx, ns, 1);
    // Read back for the stamps of this first status.
    const value = exposed((await tx.get<StoredRun>(key))!);
    tx.put(rk, { hash, value } satisfies Receipt);
    collectLater(tx, now + policy.retentionMs, rk);
    return value;
  });
}
