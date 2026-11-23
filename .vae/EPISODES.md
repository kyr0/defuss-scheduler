# Episodes

<!-- Newest last. The gate appends FAIL|DONE|FINDING; past 100 entries it trims only its own oldest DONE|FAIL|learned
FINDING (git keeps them). Leads (LESSON, FINDING learn=none) stay until wrap settles them; doctor fails above 30.
Agents append `<UTC ISO> s=<session> LESSON <VAE-DIALECT>` only for a falsified HYPOTHESIS, a dead end, or a root cause.
Session start injects the 3 newest open entries: LESSON, FINDING learn=none, FAIL without a later DONE.
Wrap: EVERY lead becomes a test | .vae/VERIFY.py rule | one MEMORY line with BC, then delete; OR delete BC evidence. -->

2026-11-22T20:46:54Z s=schedule FAIL tests.integration.1,tests.e2e.1
2026-11-22T20:48:25Z s=schedule DONE fp=525a8f0b2805 cov=92.1% paths=.github/workflows/verify.yml,AGENTS.md,ARCH.md,Makefile(+24)
2026-11-22T20:48:25Z s=schedule FINDING src/leases.ts:claim learn=test: The real HTTP slow-handler test and shared-group contention suite guard the held-group paths.
2026-11-22T20:48:25Z s=schedule FINDING scripts/consumer-types.ts:typedConsumer learn=test: A permissive worker ID type makes the consumer fixture fail due to an unused directive.
2026-11-22T20:48:25Z s=schedule FINDING scripts/bench.ts:measure learn=test: The fixture asserts its expected run count before starting and requires all eligible jobs to complete.
2026-11-22T22:17:08Z s=433bf258 DONE fp=b0453ab1a42e cov=93.9% paths=.github/workflows/verify.yml,AGENTS.md,ARCH.md,Makefile(+29)
2026-11-22T22:17:08Z s=433bf258 FINDING src/validation.ts:fingerprint|encode learn=verifier: make lint runs tsc -p tsconfig.browser.json without Node typings; appending a Buffer use to validation.ts made it fail with TS2591.
2026-11-22T22:17:08Z s=433bf258 FINDING src/store.ts:Mount learn=test: scripts/e2e.ts bundles the packed package with Bun and runs window, dedicated worker, second window, owner loss, remote worker and browser restart in Chromium 1
2026-11-22T22:17:08Z s=433bf258 FINDING src/browser.ts:own learn=test: scripts/consumer-browser.ts reopen() opens the store again directly after an awaited close in all three engines.
2026-11-22T22:17:08Z s=433bf258 FINDING src/leases.ts:claim learn=verifier: .vae/VERIFY.py rule scheduler.auto-compact rejects autoCompact: false in src/store.ts. A timing-free regression test is not available because compaction runs in
2026-11-22T22:17:08Z s=433bf258 FINDING src/store.ts:Records learn=test: tests/scheduler.test.ts 'histories record every status and edit with timestamps' plus history assertions in scripts/consumer.ts inspect and scripts/consumer-bro
2026-11-22T22:17:08Z s=433bf258 FINDING src/leases.ts:deadline learn=test: The history test renews twice and admits an occurrence, then asserts five run statuses and one schedule version.
2026-11-22T22:17:08Z s=433bf258 FINDING src/store.ts:history learn=test: The history test asserts the exact sequence succeeded, leased, retryWait, leased, ready on fast local commits.
2026-11-22T22:17:08Z s=433bf258 FINDING package.json:dependencies learn=verifier: .vae/VERIFY.py rule scheduler.bundled-store rejects defuss-db under dependencies.
2026-11-22T22:17:08Z s=433bf258 FINDING scripts/e2e.ts:browserRound learn=test: scripts/e2e.ts records every request path per browser context and requires none to match compressor.
2026-11-22T22:17:08Z s=433bf258 FINDING docs/WORKERS.md:Backplane learn=none: Documentation wording; no code path.
2026-11-22T22:17:08Z s=433bf258 FINDING README.md:Where the data lives learn=none: Design consequence of a human requirement, stated with its measurement; an opt-out is the human's decision.
2026-11-22T22:45:36Z s=433bf258 DONE fp=1048fd2bdd33 cov=93.9% paths=.github/workflows/verify.yml,AGENTS.md,ARCH.md,Makefile(+29)
2026-11-23T11:50:28Z s=433bf258 DONE fp=6ba3cb764a27 cov=94.8% paths=.github/workflows/verify.yml,AGENTS.md,ARCH.md,Makefile(+30)
2026-11-23T11:50:28Z s=433bf258 FINDING src/termination.ts:terminate learn=test: Three store tests cover the backlog an edit answers with BUSY, every target and error, the claim that arrives between stop and sweep, and the abort of a running
2026-11-23T11:50:28Z s=433bf258 FINDING src/model.ts:keys.scheduleRefs|runRefs learn=test: tests/scheduler.test.ts 'a ref names schedules and their runs across schedule IDs' covers reuse after removal, order, paging, a foreign cursor, the fixed ref an
2026-11-23T11:50:28Z s=433bf258 FINDING README.md:Workers learn=none: Documentation wording; no code path.
2026-11-23T19:47:32Z s=433bf258 DONE fp=209d3da4d912 cov=94.8% paths=.github/workflows/verify.yml,AGENTS.md,ARCH.md,Makefile(+30)
2026-11-23T19:47:32Z s=433bf258 FINDING src/leases.ts:claim|collectSlot learn=test: tests/scheduler.test.ts: a slot record and a parallel run's group last one retention period, a held lease longer.
2026-11-23T19:47:32Z s=433bf258 FINDING src/leases.ts:retire learn=test: The same test lists the ledger of a drained store and expects only the two markers, one cursor and one counter.
2026-11-23T19:47:32Z s=433bf258 FINDING docs/API.md:60 P learn=none: Documentation wording; no code path.
2026-11-23T20:18:33Z s=433bf258 DONE fp=a6f2277f97c3 cov=94.8% paths=.github/workflows/verify.yml,AGENTS.md,ARCH.md,Makefile(+30)
2026-11-23T20:18:33Z s=433bf258 FINDING src/scheduler.ts:maintain|src/store.ts:erase learn=test: tests/scheduler.test.ts: the history test expects an empty run history after retention and a restart; the drained-store test expects an empty runs collection an
2026-11-23T20:18:33Z s=433bf258 FINDING src/scheduler.ts:maintain learn=memory: .vae/MEMORY.md records why the erasure stays outside the transaction; a test with thousands of attempts would take about 25 s per run.
2026-11-23T20:18:33Z s=433bf258 FINDING scripts/consumer-browser.ts:thread learn=test: scripts/e2e.ts runs the scenario in three engines on every make e2e.
2026-11-23T20:18:33Z s=433bf258 FINDING ARCH.md:88 B learn=none: A measured value in documentation; it changes with each engine version and is re-measured then.
2026-11-23T20:18:33Z s=433bf258 FINDING README.md:155 P|docs/API.md:44 P|src/types.ts:runHistory learn=none: Documentation wording that follows the behavior change; the store test asserts the behavior.
2026-11-23T20:45:37Z s=433bf258 DONE fp=5e1bc9a75e39 cov=94.8% paths=.github/workflows/verify.yml,AGENTS.md,ARCH.md,Makefile(+30)
2026-11-23T20:45:37Z s=433bf258 FINDING src/scheduler.ts:arm|maintain learn=test: tests/scheduler.test.ts: a retention backlog is collected without a tick of waiting per page.
2026-11-23T20:45:37Z s=433bf258 FINDING README.md:226 P|docs/API.md:10 P learn=none: Documentation wording that follows the behavior change; the store test asserts the behavior.
