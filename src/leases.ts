/** VERIFIED: Ready entries include selection metadata. A claim checks a blocked group once per page and commits ownership before returning a grant. An empty answer writes nothing unless it moves the slot's scan position. */
import {
  collectLater,
  count,
  exposed,
  keys,
  enqueue,
  openKey,
  runRefKey,
  type ReadyEntry,
  type Policy,
  type StoredRun,
} from "./model.js";
import type { Ledger, Records } from "./store.js";
import {
  type Lease,
  type Report,
  type Claim,
  type ClaimResult,
  type Grant,
  type Json,
  type Outcome,
  type ScheduleRecord,
  SchedulerError,
} from "./types.js";
import {
  encode,
  fingerprint,
  id,
  integer,
  invalid,
  json,
  names,
  object,
  timeKey,
} from "./validation.js";

interface Group {
  fence: string;
  runId?: string;
  token?: string;
  expiresAt?: number;
}
interface Slot {
  scope: string;
  /** The wall time of the record's last write, from which its retention counts. */
  at: number;
  /** The sequence and input hash of the slot's last grant. An empty answer leaves both alone. */
  sequence: number;
  hash: string;
  runId?: string;
  token?: string;
  /** Where the next scan continues after a full page without a grant. */
  cursor?: string;
}
interface Ack {
  hash: string;
  outcome: Outcome;
  action: string;
}
export function runId(value: unknown): string {
  if (typeof value !== "string" || !/^[a-zA-Z0-9_-]{1,1800}$/.test(value))
    invalid("Invalid run ID");
  return value;
}
function identity(l: Lease) {
  object(l, ["workerId", "sessionId", "slot", "runId", "token", "fence"]);
  id(l.workerId);
  id(l.sessionId);
  integer(l.slot, 0, 65535);
  runId(l.runId);
  id(l.token);
  if (typeof l.fence !== "string" || !/^[1-9]\d{0,79}$/.test(l.fence))
    invalid("Invalid lease fence");
}
function matches(a: Lease | undefined, b: Lease) {
  return (
    a &&
    a.token === b.token &&
    a.fence === b.fence &&
    a.workerId === b.workerId &&
    a.sessionId === b.sessionId &&
    a.slot === b.slot &&
    a.runId === b.runId
  );
}
function granted(run: StoredRun, expiresAt: number, now: number): Grant {
  return {
    kind: "granted",
    run: exposed(run),
    lease: run.lease!,
    expiresAt,
    ttlMs: Math.max(0, expiresAt - now),
  };
}
/**
 * A lease lasts while its group record names it. A renewal moves only that record and the expiry index,
 * so a run's document, and with it the run's history, changes with its status alone.
 */
async function deadline(tx: Records, run: StoredRun): Promise<number> {
  const group = await tx.get<Group>(run.group);
  return group?.token === run.lease?.token && group?.runId === run.public.id
    ? group!.expiresAt!
    : 0;
}
async function release(tx: Records, run: StoredRun) {
  const lease = run.lease!;
  const group = await tx.get<Group>(run.group);
  if (
    group?.token !== lease.token ||
    group.fence !== lease.fence ||
    group.runId !== lease.runId
  )
    throw new SchedulerError("FORMAT", "Persisted group ownership mismatch");
  tx.put(run.group, { fence: group.fence });
  tx.del(keys.expiry(group.expiresAt!, run.scope, run.public.id));
  delete run.lease;
}
/**
 * What every terminal status shares: the run leaves the unfinished set and is collected, with its ref entry,
 * after retention. No later grant can raise the fence of a parallel run's own group, so that record goes now.
 */
async function retire(
  tx: Records,
  run: StoredRun,
  wall: number,
  policy: Policy,
) {
  collectLater(
    tx,
    wall + policy.retentionMs,
    keys.run(run.scope, run.public.id),
    ...(run.public.ref === undefined ? [] : [runRefKey(run)]),
  );
  tx.del(openKey(run));
  if (run.group === keys.ownGroup(run.scope, run.public.id)) tx.del(run.group);
  await count(tx, run.scope, -1);
}
/**
 * A slot record is the receipt of its claims and is kept as long as other receipts: one retention period
 * after its last write. Renewals do not write it, so a slot that still holds a lease is asked again later.
 */
export async function collectSlot(
  tx: Records,
  key: string,
  wall: number,
  policy: Policy,
) {
  const slot = await tx.get<Slot>(key);
  if (!slot) return;
  const held =
    slot.runId !== undefined &&
    (await tx.get<StoredRun>(keys.run(slot.scope, slot.runId)))?.lease
      ?.token === slot.token;
  const due = (held ? wall : slot.at) + policy.retentionMs;
  if (due > wall) collectLater(tx, due, key);
  else tx.del(key);
}
/** A hard stop of one unfinished run. A lease it held ends here, so its holder's next report is stale. */
export async function terminateRun(
  tx: Records,
  run: StoredRun,
  wall: number,
  policy: Policy,
  cause: string,
) {
  const item = run.public;
  if (run.lease) await release(tx, run);
  else tx.del(keys.ready(run));
  item.state = "terminated";
  item.terminationCause = cause;
  item.finishedAt = wall;
  await retire(tx, run, wall, policy);
  tx.put(keys.run(run.scope, item.id), run);
}
async function settle(
  tx: Records,
  run: StoredRun,
  wall: number,
  policy: Policy,
  completion:
    | { success: true; value: Json }
    | { success: false; error: string; permanent: boolean },
) {
  const item = run.public;
  await release(tx, run);
  if (!completion.success) item.lastError = completion.error;
  if (
    !completion.success &&
    !completion.permanent &&
    item.attempt < item.retry.maxAttempts
  ) {
    const max = Math.min(
      item.retry.maxMs,
      item.retry.baseMs * 2 ** Math.min(item.attempt - 1, 40),
    );
    item.availableAt = wall + Math.floor(max * (0.5 + Math.random() * 0.5));
    item.state = "retryWait";
    enqueue(tx, run);
  } else {
    item.finishedAt = wall;
    item.state = completion.success ? "succeeded" : "dead";
    if (completion.success) item.result = completion.value;
    await retire(tx, run, wall, policy);
  }
  tx.put(keys.run(run.scope, item.id), run);
}
export async function expire(
  tx: Records,
  ns: string,
  rid: string,
  ledger: Ledger,
  policy: Policy,
) {
  const run = await tx.get<StoredRun>(keys.run(ns, rid));
  if (run?.lease && (await deadline(tx, run)) <= ledger.leaseNow())
    await settle(tx, run, ledger.wallNow(), policy, {
      success: false,
      error: "Lease expired",
      permanent: false,
    });
}
export async function claim(
  ledger: Ledger,
  ns: string,
  input: Claim,
  policy: Policy,
): Promise<ClaimResult> {
  object(input, [
    "workerId",
    "sessionId",
    "slot",
    "sequence",
    "requestId",
    "handlers",
    "capabilities",
    "maxPayloadBytes",
  ]);
  const { workerId, sessionId, slot, sequence } = input;
  id(workerId);
  id(sessionId);
  id(input.requestId);
  integer(slot, 0, 65535);
  integer(sequence, 1, Number.MAX_SAFE_INTEGER);
  const handlers = names(input.handlers),
    capabilities = names(input.capabilities ?? []);
  if (handlers.length === 0) invalid("At least one handler is required");
  const ceiling = integer(
    input.maxPayloadBytes ?? Number.MAX_SAFE_INTEGER,
    0,
    Number.MAX_SAFE_INTEGER,
  );
  const slotKey = keys.slot(ns, input),
    hashing = fingerprint(input);
  const empty = { kind: "empty", retryAfterMs: policy.pollMs } as const;
  const gone = { kind: "superseded", retryAfterMs: policy.pollMs } as const;
  const result = await ledger.write<ClaimResult>(async (tx) => {
    const hash = await hashing;
    const prev = await tx.get<Slot>(slotKey);
    const save = (slot: Omit<Slot, "scope" | "at">) => {
      const at = ledger.wallNow();
      tx.put(slotKey, { ...slot, scope: ns, at } satisfies Slot);
      // One collection entry follows the record from its first write to its deletion.
      if (!prev) collectLater(tx, at + policy.retentionMs, slotKey);
    };
    if (prev && sequence < prev.sequence) return gone;
    if (prev && sequence === prev.sequence && prev.hash !== hash)
      throw new SchedulerError(
        "REVISION_CONFLICT",
        "Claim sequence reused with different input",
      );
    if (prev?.runId) {
      const old = await tx.get<StoredRun>(keys.run(ns, prev.runId));
      const until =
        old?.lease?.token === prev.token ? await deadline(tx, old!) : 0;
      if (until > ledger.leaseNow()) {
        if (sequence === prev.sequence)
          return granted(old!, until, ledger.leaseNow());
        throw new SchedulerError("BUSY", "Worker slot already holds a lease");
      }
      await expire(tx, ns, prev.runId, ledger, policy);
    }
    // A sequence acquires one job at most, so the replay of a finished grant gets nothing.
    if (prev && sequence === prev.sequence) return gone;
    const prefix = "q/" + ns + "/";
    const upper = prefix + timeKey(ledger.wallNow()) + "/\uffff";
    const rows = await tx.rows<ReadyEntry>(
      prefix,
      policy.pageSize + 1,
      prev?.cursor,
      upper,
    );
    const blockedGroups = new Set<string>();
    for (const row of rows.slice(0, policy.pageSize)) {
      const candidate = row.value;
      if (
        !handlers.includes(candidate.handler) ||
        candidate.payloadBytes > ceiling ||
        !candidate.capabilities.every((c) => capabilities.includes(c)) ||
        blockedGroups.has(candidate.group)
      )
        continue;
      const group = await tx.get<Group>(candidate.group);
      if (group?.runId) {
        if (group.expiresAt! > ledger.leaseNow()) {
          blockedGroups.add(candidate.group);
          continue;
        }
        await expire(tx, ns, group.runId, ledger, policy);
        if ((await tx.get<Group>(candidate.group))?.runId) continue;
      }
      const key = keys.run(ns, candidate.id);
      const run = await tx.get<StoredRun>(key);
      if (
        !run ||
        !["ready", "retryWait"].includes(run.public.state) ||
        keys.ready(run) !== row.key ||
        run.group !== candidate.group
      )
        throw new SchedulerError("FORMAT", "Ready index disagrees with run");
      const item = run.public;
      // A hard stop of the schedule reaches every run its sweep has not come to yet.
      const owner = await tx.get<ScheduleRecord>(
        keys.schedule(ns, item.scheduleId),
      );
      if (owner?.state === "terminated") {
        await terminateRun(
          tx,
          run,
          ledger.wallNow(),
          policy,
          owner.terminationCause!,
        );
        continue;
      }
      const token = crypto.randomUUID(),
        fence = (BigInt(group?.fence ?? "0") + 1n).toString();
      const now = ledger.leaseNow(),
        expiresAt = now + policy.leaseMs;
      run.lease = { workerId, sessionId, slot, runId: item.id, token, fence };
      item.state = "leased";
      item.attempt++;
      tx.del(row.key);
      tx.put(key, run);
      tx.put(run.group, { runId: item.id, token, fence, expiresAt });
      tx.put(keys.expiry(expiresAt, ns, item.id), { scope: ns, id: item.id });
      save({ sequence, hash, runId: item.id, token });
      // Read back for the stamps of this status change.
      return granted((await tx.get<StoredRun>(key))!, expiresAt, now);
    }
    const cursor =
      rows.length > policy.pageSize ? rows[policy.pageSize - 1].key : undefined;
    // Every slot polls again and again while nothing is due. The answer is written only when it moves the
    // saved scan position or drops a finished run, so those polls commit nothing.
    if (cursor !== prev?.cursor || prev?.runId)
      save({
        sequence: prev?.sequence ?? 0,
        hash: prev?.hash ?? "",
        ...(cursor && { cursor }),
      });
    return cursor ? { ...empty, retryAfterMs: 1 } : empty;
  });
  if (result.kind === "granted") {
    result.ttlMs = Math.max(0, result.expiresAt - ledger.leaseNow());
    if (!result.ttlMs) return gone;
  }
  return result;
}

export async function report(
  ledger: Ledger,
  ns: string,
  action: "renew" | "complete" | "fail",
  input: Report & { result?: Json; error?: string; permanent?: boolean },
  policy: Policy,
): Promise<Outcome> {
  object(input, [
    "lease",
    "requestId",
    ...(action === "complete"
      ? ["result"]
      : action === "fail"
        ? ["error", "permanent"]
        : []),
  ]);
  identity(input.lease);
  id(input.requestId);
  if (
    action === "fail" &&
    (typeof input.error !== "string" ||
      !input.error ||
      input.error.length > 4096 ||
      (input.permanent !== undefined && typeof input.permanent !== "boolean"))
  )
    invalid("Invalid failure report");
  const value =
    action === "complete"
      ? json(input.result === undefined ? null : input.result)
      : null;
  const l = input.lease,
    hashing = fingerprint([action, input]),
    rk = "a/" + ns + "/" + encode([l.token, input.requestId]);
  const outcome = await ledger.write<Outcome>(async (tx) => {
    const hash = await hashing;
    const ack = await tx.get<Ack>(rk);
    if (ack && ack.hash !== hash)
      throw new SchedulerError("REVISION_CONFLICT", "Report request ID reused");
    if (ack && action !== "renew")
      return { ...ack.outcome, kind: "alreadyApplied" };
    const key = keys.run(ns, l.runId),
      run = await tx.get<StoredRun>(key);
    if (!run) return { kind: "unknown" };
    if (!matches(run.lease, l)) return { kind: "stale" };
    const group = await tx.get<Group>(run.group);
    if (
      group?.token !== l.token ||
      group.fence !== l.fence ||
      group.runId !== l.runId
    )
      return { kind: "stale" };
    if (group.expiresAt! <= ledger.leaseNow()) {
      await expire(tx, ns, l.runId, ledger, policy);
      return { kind: "stale" };
    }
    if (ack) return { kind: "alreadyApplied", expiresAt: group.expiresAt };
    const applied: Outcome = { kind: "applied" };
    if (action === "renew") {
      const expiresAt = ledger.leaseNow() + policy.leaseMs;
      tx.del(keys.expiry(group.expiresAt!, ns, l.runId));
      tx.put(keys.expiry(expiresAt, ns, l.runId), { scope: ns, id: l.runId });
      tx.put(run.group, { ...group, expiresAt });
      applied.expiresAt = expiresAt;
    } else {
      await settle(
        tx,
        run,
        ledger.wallNow(),
        policy,
        action === "complete"
          ? { success: true, value }
          : {
              success: false,
              error: input.error!,
              permanent: input.permanent ?? false,
            },
      );
    }
    tx.put(rk, { hash, outcome: applied, action });
    collectLater(tx, ledger.wallNow() + policy.retentionMs, rk);
    return applied;
  });
  if ("expiresAt" in outcome && outcome.expiresAt !== undefined) {
    outcome.ttlMs = Math.max(0, outcome.expiresAt - ledger.leaseNow());
    if (!outcome.ttlMs) return { kind: "stale" };
  }
  return outcome;
}
