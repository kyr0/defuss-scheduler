/** VERIFIED: A stored cutoff bounds an edit while catch-up pages drain. A full-queue test checks that three old occurrences keep their original payload. A hard stop skips that drain. */
import { calendar } from "./calendar.js";
import {
  count,
  keys,
  enqueue,
  collectLater,
  definition,
  loadSchedule,
  openKey,
  runRefKey,
  schedule,
  type Draft,
  type Policy,
  type StoredRun,
} from "./model.js";
import type { Ledger, Records } from "./store.js";
import {
  type Schedule,
  type Change,
  type Definition,
  type Patch,
  SchedulerError,
} from "./types.js";
import {
  byteLength,
  encode,
  fingerprint,
  id,
  invalid,
  object,
} from "./validation.js";

type Action = "update" | "pause" | "resume" | "remove";
interface Pending {
  action: Action;
  cutoff: number;
  replacement: Draft;
  hash: string;
  receiptKey: string;
}
interface Receipt {
  hash: string;
  value: Schedule;
}
function checked(receipt: Receipt, hash: string): Schedule {
  if (receipt.hash !== hash)
    throw new SchedulerError(
      "REVISION_CONFLICT",
      "Request ID reused with different arguments",
    );
  return receipt.value;
}
function receipt(
  tx: Records,
  key: string,
  hash: string,
  value: Schedule,
  at: number,
) {
  tx.put(key, { hash, value });
  collectLater(tx, at, key);
}
function index(
  tx: Records,
  ns: string,
  previous: Draft | undefined,
  current: Draft,
) {
  if (previous?.nextDue != null)
    tx.del(keys.due(previous.nextDue, ns, previous.id));
  const { nextDue, lastProcessed, lastSkipped, ...record } = current;
  const key = keys.schedule(ns, current.id);
  // Every edit takes a fresh revision, so an unchanged one means only the cursor moved.
  if (previous?.revision !== current.revision) tx.put(key, record);
  if (!previous && current.ref !== undefined)
    tx.put(keys.scheduleRefs(ns, current.ref) + encode(current.id), key);
  tx.put("c/" + key, { nextDue, lastProcessed, lastSkipped });
  if (current.nextDue !== null)
    tx.put(keys.due(current.nextDue, ns, current.id), {
      scope: ns,
      id: current.id,
    });
}
async function materialize(
  tx: Records,
  ns: string,
  s: Draft,
  cutoff: number,
  policy: Policy,
) {
  if (s.nextDue === null || s.nextDue > cutoff) return;
  const expr = calendar(s.cron, s.timezone);
  const old = { ...s };
  const next = (after: number) =>
    expr.nextRun(new Date(after))?.getTime() ?? null;
  const last = (through: number) =>
    expr
      .previousRuns(1, new Date(Math.floor(through / 1000) * 1000 + 1000))[0]
      ?.getTime();
  let due: number | null = s.nextDue;
  if (s.misfire === "skip") {
    const earliest = cutoff - s.graceMs;
    if (due < earliest) {
      const end = last(earliest - 1) ?? due;
      s.lastSkipped = { from: due, through: end };
      s.lastProcessed = end;
      due = next(end);
    }
  }
  const quota = policy.maxPending - (await count(tx, ns));
  if (s.misfire === "latest" && quota > 0 && due !== null && due <= cutoff) {
    const target = Math.max(due, last(cutoff) ?? due);
    if (target > due)
      s.lastSkipped = { from: due, through: last(target - 1) ?? due };
    due = target;
  }
  let added = 0;
  while (
    due !== null &&
    due <= cutoff &&
    added < quota &&
    added < policy.pageSize
  ) {
    const rid = encode([ns, s.id, due]);
    const key = keys.run(ns, rid);
    if (await tx.get(key))
      throw new SchedulerError(
        "FORMAT",
        "Occurrence already exists ahead of cursor",
      );
    const shared =
      s.concurrencyKey !== undefined
        ? ["named", s.concurrencyKey]
        : ["schedule", s.id];
    const run: StoredRun = {
      scope: ns,
      group: s.parallel
        ? keys.ownGroup(ns, rid)
        : "g/" + ns + "/" + encode(shared),
      public: {
        id: rid,
        scheduleId: s.id,
        ...(s.ref !== undefined && { ref: s.ref }),
        scheduleRevision: s.revision,
        scheduledAt: due,
        handler: s.handler,
        payload: s.payload,
        capabilities: s.capabilities,
        payloadBytes: byteLength(JSON.stringify(s.payload)),
        retry: s.retry,
        attempt: 0,
        availableAt: due,
        state: "ready",
        lastError: null,
        terminationCause: null,
        finishedAt: null,
        result: null,
      },
    };
    tx.put(key, run);
    enqueue(tx, run);
    tx.put(openKey(run), key);
    if (s.ref !== undefined) tx.put(runRefKey(run), key);
    added++;
    s.lastProcessed = due;
    due = next(due);
  }
  if (added) await count(tx, ns, added);
  s.nextDue = due;
  if (s.nextDue !== old.nextDue) index(tx, ns, old, s);
}

export async function advance(
  tx: Records,
  ns: string,
  name: string,
  now: number,
  policy: Policy,
) {
  const k = keys.schedule(ns, name);
  const current = await loadSchedule(tx, k);
  if (!current || current.state !== "active") return;
  const pendingKey = "p/" + k;
  const pending = await tx.get<Pending>(pendingKey);
  const cutoff = pending?.cutoff ?? now;
  await materialize(tx, ns, current, cutoff, policy);
  if (pending && (current.nextDue === null || current.nextDue > cutoff)) {
    const replacement = {
      ...pending.replacement,
      lastSkipped: current.lastSkipped,
    };
    index(tx, ns, current, replacement);
    receipt(
      tx,
      pending.receiptKey,
      pending.hash,
      (await loadSchedule(tx, k))!,
      now + policy.retentionMs,
    );
    tx.del(pendingKey);
  }
}

/**
 * A hard stop ends an active or paused schedule where it stands. Its unadmitted occurrences are dropped
 * and an edit that was draining ends with it, which is what an edit's own barrier cannot do.
 */
export async function halt(
  tx: Records,
  ns: string,
  key: string,
  cause: string,
): Promise<{ id: string; stopped: boolean }> {
  const current = await loadSchedule(tx, key);
  if (!current) throw new SchedulerError("NOT_FOUND", "Schedule absent");
  const stopped = current.state === "active" || current.state === "paused";
  if (stopped) {
    tx.del("p/" + key);
    index(tx, ns, current, {
      ...current,
      state: "terminated",
      terminationCause: cause,
      nextDue: null,
      revision: crypto.randomUUID(),
    });
  }
  return { id: current.id, stopped };
}

export function create(
  ledger: Ledger,
  ns: string,
  input: Definition,
  options: { requestId: string },
  policy: Policy,
) {
  object(options, ["requestId"]);
  const rk = "m/" + ns + "/" + encode(id(options.requestId));
  const hashing = fingerprint(["create", input]);
  return ledger.write(async (tx) => {
    const hash = await hashing;
    const known = await tx.get<Receipt>(rk);
    if (known) return checked(known, hash);
    const now = ledger.wallNow();
    const draft = schedule(input, now);
    const key = keys.schedule(ns, draft.id);
    if (await tx.get(key))
      throw new SchedulerError("ALREADY_EXISTS", "Schedule ID is reserved");
    index(tx, ns, undefined, draft);
    const value = (await loadSchedule(tx, key))!;
    receipt(tx, rk, hash, value, now + policy.retentionMs);
    return value;
  });
}

export async function modify(
  ledger: Ledger,
  ns: string,
  name: string,
  action: Action,
  patch: Patch,
  change: Change,
  policy: Policy,
): Promise<Schedule> {
  object(change, ["revision", "requestId"]);
  id(change.revision);
  id(name);
  object(patch, [
    "cron",
    "timezone",
    "handler",
    "payload",
    "capabilities",
    "retry",
    "misfire",
    "graceMs",
  ]);
  const k = keys.schedule(ns, name),
    pk = "p/" + k;
  const rk = "m/" + ns + "/" + encode(id(change.requestId));
  const hashing = fingerprint([action, name, patch, change]);
  const result = await ledger.write(async (tx) => {
    const hash = await hashing;
    const known = await tx.get<Receipt>(rk);
    if (known) return checked(known, hash);
    const current = await loadSchedule(tx, k);
    if (
      !current ||
      current.state === "removed" ||
      current.state === "terminated"
    )
      throw new SchedulerError(
        "NOT_FOUND",
        "Schedule absent, removed or terminated",
      );
    if (current.revision !== change.revision)
      throw new SchedulerError("REVISION_CONFLICT", "Revision changed");
    const pending = await tx.get<Pending>(pk);
    if (pending && (pending.hash !== hash || pending.receiptKey !== rk))
      throw new SchedulerError(
        "BUSY",
        "Prior edit is draining its calendar window",
      );
    const now = ledger.wallNow();
    if (!pending) {
      if (action === "resume" && current.state !== "paused")
        invalid("Resume requires a paused schedule");
      if (action === "pause" && current.state !== "active")
        invalid("Pause requires an active schedule");
      const cutoff = Math.max(now, current.lastProcessed);
      const updated = schedule(
        {
          ...definition(current),
          ...patch,
          retry: { ...current.retry, ...patch.retry },
        },
        cutoff,
      );
      updated.state =
        action === "remove"
          ? "removed"
          : action === "pause"
            ? "paused"
            : action === "resume"
              ? "active"
              : current.state;
      if (updated.state !== "active") updated.nextDue = null;
      updated.revision = crypto.randomUUID();
      updated.lastSkipped = current.lastSkipped;
      if (current.state === "paused") {
        index(tx, ns, current, updated);
        const value = (await loadSchedule(tx, k))!;
        receipt(tx, rk, hash, value, now + policy.retentionMs);
        return value;
      }
      tx.put(pk, {
        action,
        cutoff,
        replacement: updated,
        hash,
        receiptKey: rk,
      } satisfies Pending);
    }
    await advance(tx, ns, name, now, policy);
    return (await tx.get<Receipt>(rk))?.value;
  });
  if (!result)
    throw new SchedulerError(
      "BUSY",
      "Edit accepted and draining. Retry with the same request ID",
    );
  return result;
}
