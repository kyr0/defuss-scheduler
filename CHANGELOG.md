# Changelog

## Unreleased

The worker-thread docs and the packed consumer use defuss-multicore 0.1.1, whose pools answer through `node:worker_threads` under Bun too.

## 0.3.0

Added one-off jobs: `Queue.submit(job)` and `Queue.getJob(id)`. A job is a run without a cron. It can have a deadline (`timeoutMs`), after which it ends as `timedOut`, and a follow-up job for each ending in `on`. The follow-up enters the queue in the transaction that ends the job and carries `cause: { job, run, state }`. `terminate({ job })` stops one job, and `terminate({ ref })` reaches jobs too.

Added `poolHandlers(pool, names)`, which runs handlers as tasks of a worker-thread pool such as defuss-multicore's.

Breaking: `Run.scheduleId` and `Run.scheduleRevision` are null for jobs and follow-ups, and `Run.state` can be `timedOut`. Runs gained `jobId`, `deadline`, `on` and `cause`; stores written by 0.2.0 read with these fields null.

## 0.2.0

Added `Scheduler.ready` for workers that serve every scope.

## 0.1.0

Initial release.
