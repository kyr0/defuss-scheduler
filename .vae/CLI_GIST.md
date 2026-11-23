# CLI gist

<!-- Commands future agents need that the Makefile verbs do not cover (setup, env, data, deploy):
- VERIFIED[purpose] `cmd`: only after an observed success
- UNKNOWN[purpose] BC gap
Budget 2 KiB (`vae.py doctor --repo .`); entries are injected at session start. -->
- VERIFIED[toolchain] `eval "$(mise env)"` before any make verb: the PATH bun was 1.4.3 while mise.toml pins 1.4.5 BC observed 2026-11-22
- VERIFIED[agent sandbox: install] `BUN_INSTALL_CACHE_DIR="$TMPDIR/bun-cache" bun install --frozen-lockfile`: the default bun cache is read-only there (EPERM) BC observed 2026-11-22
- VERIFIED[agent sandbox: test|coverage|e2e|gate] run them outside the sandbox: `make test` binds loopback (Bun.serve fails EADDRINUSE inside) and `make e2e` launches Playwright browsers BC observed 2026-11-22
