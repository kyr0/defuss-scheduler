# Scheduler API

All methods on a scoped queue return promises. Public data uses JSON values. A payload or result can contain up to 128 KiB of UTF-8 JSON at depth 48. Identifiers accept 1 to 128 UTF-8 bytes. A scope consists of app, tenant and bucket. Names and group keys are isolated across all three fields.

## Authority

| Option | Default | Unit or behavior |
| --- | --- | --- |
| directory | required | Node.js and Bun: a local filesystem directory. Browser: a store name of 1 to 64 characters from a-z, 0-9, `_` and `-`. One exclusive owner. |
| tickMs | 250 | Delay in milliseconds between finished maintenance turns. A turn that filled its retention page is followed by the next one at once. |
| pollMs | 250 | Suggested worker delay after an empty scan. |
| leaseMs | 30000 | Lease duration in milliseconds. Supported range 50 through 86400000. |
| pageSize | 32 | Maintenance and claim scan budget per page. Range 1 through 128. |
| maxPending | 100000 | Maximum nonterminal occurrences per scope. Saturation leaves the schedule cursor in place. |
| retentionMs | 86400000 | Retention of terminal runs, operation receipts and worker-slot records in milliseconds. Minimum 1000. Cleanup runs in bounded pages. A run is removed with its history. |
| autoTick | true | Set false for a caller-driven loop. Startup still performs one recovery turn. |
| clock | system clocks | Optional function returning epoch milliseconds for deterministic simulations. It replaces both calendar and lease time. Record timestamps and history always use the system clock. |
| onTransaction | optional | Callback with queueMs, transactionMs and DBSONL conflict retries per successful transaction. Keep the callback short. |
| onError | console.error | Receives background maintenance faults. Explicit tick calls reject on failure. |

Call scope with the three name components. ready takes handlers and an optional limit of 1 to 128 (default 32) and returns the scopes that hold a run of one of those handlers that a claim would grant now, earliest first. It reads at most 512 index entries per handler and writes nothing. tick performs one bounded maintenance turn. Concurrent ticks share that turn. The database compacts its log by itself. compact rewrites live records and collects old generations at once, while the authority serializes its commands. close drains work already admitted and releases the store's lock.

## Calendar

Use five fields for minute, hour, day of month, month and day of week. A sixth field at the front specifies seconds. Numeric lists, ranges, wildcards and positive steps are supported. Weekday zero and seven mean Sunday. When both day fields are restricted, either match qualifies. Schedules default to UTC. Use an explicit IANA timezone for local-time schedules. nextOccurrence takes a cron string, timezone and reference Date or epoch milliseconds. It returns the UTC millisecond strictly after the reference or null when the expression has no future occurrence.

| Expression in Europe/Berlin | Calendar result |
| --- | --- |
| 30 2 * * * on 29 March 2026 | 03:00 local, 01:00 UTC, the first valid instant after the spring gap |
| 30 2 * * * on 25 October 2026 | First 02:30 local, 00:30 UTC |
| */30 * * * * on 25 October 2026 | Both passes of the repeated hour, in UTC order |

## Schedule management

| Method | Effect |
| --- | --- |
| create(definition, options?) | Requires an unused ID. Starts after the authority's sampled creation time. An optional object with requestId allows exact replay. |
| get(id), list(page?) | Read one definition or a keyset page. A missing ID returns null. Removed and terminated schedules retain their reserved ID and status. |
| update(id, patch, change) | Change cron, timezone, handler, payload, capabilities, retry or misfire configuration. ref, concurrencyKey and parallel are fixed at creation. Each admitted occurrence retains its original inputs. |
| pause(id, change), resume(id, change) | Stop calendar production or resume strictly after the resume cutoff. Previously admitted work remains eligible. |
| remove(id, change) | Stop future work and reserve the schedule ID permanently. Admitted runs retain their lifecycle. |
| submit(job, options?) | Admit a one-off job under an ID unused while its record exists. An optional object with requestId allows exact replay. Returns the job's run. |
| getJob(id) | Read a job's run by the job's ID. A missing or collected job returns null. |
| getRun(runId), listRuns(page?) | Read public occurrence state, including attempt, error, result and schedule revision. Lease tokens are private. |
| scheduleHistory(id, options?) | List every definition and state the schedule had, newest first. The values omit nextDue, lastProcessed and lastSkipped. |
| runHistory(runId, options?) | List every status the run had, newest first. The list is empty once retention removed the run. |
| terminate(target, { cause }) | Stop at once, without the drain an edit waits for. The target is one of `{ run }`, `{ job }`, `{ schedule }`, `{ ref }` or `{ all: true }`; only `{ run }` ends a follow-up. Returns the counts of terminated schedules and runs. |

A run of a schedule has scheduleId and scheduleRevision, with jobId null. A job's run has jobId, with scheduleId and scheduleRevision null; its deadline is the epoch milliseconds at which it ends as timedOut, null without timeoutMs, and on holds its normalized follow-ups. A follow-up has all three IDs null and cause set to the job, run and ending that admitted it. Runs a 0.2.0 store wrote read with jobId, deadline, on and cause null.

Schedules and runs carry createdAt and updatedAt as epoch milliseconds. createdAt is the first write. updatedAt is the last change of a schedule's definition or state, or a run's last status change. A history entry holds from, until and value. until is null for the latest entry. value is null only for a removed run whose history is not erased yet. Both history methods accept limit, from 1 through 500 with a default of 50. A lease renewal and a move of the calendar cursor add no entry.

A definition can carry a ref of 1 to 128 UTF-8 bytes. Several schedules of a scope can share it, and each run copies it at admission. A page with ref lists only the schedules that carry it, or their runs ordered by scheduled time. Retention removes a run from that listing together with the run.

terminate marks every active or paused schedule of its target as terminated, drops a draining edit, and ends every unfinished run of the target as terminated, a leased one included. Both records store the cause, a string of 1 to 4096 characters, in terminationCause. A removed schedule keeps its state while its unfinished runs end. A run or schedule that is already final counts as zero, so the call can be repeated. An unknown run or schedule ID rejects with NOT_FOUND. A terminated schedule rejects edits with NOT_FOUND and keeps its ID reserved. The call ends runs in pages of pageSize, one transaction per page. After its first transaction no run of a terminated schedule is granted.

A change contains revision and requestId. Reuse both and the identical arguments after an ambiguous response. A different call reusing the same request ID produces REVISION_CONFLICT. Large edits may return BUSY once the edit has been durably accepted. Its cutoff stays fixed while maintenance drains the earlier calendar window. Poll by retrying the original command. Pagination accepts limit (1 through 500) and the cursor returned by the previous page in the same scope and collection. Lists use storage-key order. Each call reads a fresh snapshot.

The retry defaults are three total grants, a base delay of 500 ms and a 30000 ms cap. Backoff picks a jittered duration between half and all of the current capped exponential delay. Specify retry.maxAttempts, retry.baseMs and retry.maxMs to override it. A transient error or lease expiry uses the same budget. PermanentFailure terminates an occurrence immediately. Later occurrences can run while an earlier one waits for retry.

## Pull protocol

1. Claim with workerId, sessionId, slot, sequence, requestId and supported handlers. Optional capabilities must cover every capability on the job. maxPayloadBytes filters by UTF-8 JSON size. Each slot starts at sequence 1. Use a new session ID on process startup.
2. Retry an ambiguous claim with exactly the same request. A replay of a granted sequence returns the live grant, or superseded once that grant has ended. A sequence at or below the slot's last grant is superseded. The slot's sequences are remembered for at least retentionMs after its last grant, and for as long as it holds a lease. An empty answer writes nothing, and a retry of it scans again. Use a fresh sequence and request ID for another decision. BUSY means the slot already holds a grant. The worker helper implements this loop.
3. A grant carries the occurrence, a fresh lease identity, fence, expiresAt and ttlMs. Report with the complete lease and a requestId. Renew extends ownership. Complete accepts an optional JSON result. Fail takes an error string and optional permanent flag.
4. A report returns applied, alreadyApplied, stale or unknown. A stale result ends ownership. A terminated run answers stale. A terminal receipt can replay for its retention period. Poll once more for a pending occurrence using a fresh claim sequence.

startWorker returns done and stop. stop stops pulling and waits for in-flight reports up to drainMs (default 15000). At the deadline the handler signal aborts and stop returns. A handler that ignores cancellation may still be running. done settles when its loops actually finish. The worker uses remaining lease time and request duration to schedule renewal. For a custom WorkerPort, give transport calls bounded deadlines.

Use defineHandler(parse, handle) to validate and narrow incoming JSON. A parser exception marks the occurrence permanently failed. Handling functions receive the validated payload and a context with runId, attempt, fence and signal. Handler exceptions are transient unless the handler throws PermanentFailure.

## Fault handling

| Code | Caller action |
| --- | --- |
| INVALID_ARGUMENT | Correct the input. |
| ALREADY_EXISTS, NOT_FOUND, REVISION_CONFLICT | Read the current object or correct the request identity. |
| BUSY | Wait and retry an identical operation. |
| OWNER_BUSY | Connect to the live authority or wait for its shutdown. In a browser the owner is another window or worker of the origin. Retry the open to take over when it is gone. |
| CLOSED | Reconnect to an open scheduler. |
| FORMAT | Check store compatibility and integrity before reopening. |
| COMMIT_UNCERTAIN | Retry with the original request ID after recovery. Avoid issuing a new operation identity. |

Underlying storage and DBSONL I/O errors can also propagate. In a browser, openScheduler rejects with a plain Error where Web Locks or the origin private file system are missing, as in an insecure context. Only an acknowledged storage transition authorizes execution.
