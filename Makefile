.PHONY: help setup build pack lint test coverage integration e2e bench metrics start stop restart status log verify
.DEFAULT_GOAL := help

help: ## list commands
	@awk -F':.*## ' '/^[a-zA-Z0-9_-]+:.*## /{printf "  %-12s %s\n", $$1, $$2}' $(firstword $(MAKEFILE_LIST))
setup: ## install pinned development tools and dependencies
	@if command -v mise >/dev/null 2>&1; then mise install; fi
	bun install --frozen-lockfile
	bunx playwright-core install chromium firefox webkit
build: ## build TypeScript ESM entry points and declaration files
	bun run build
pack: build ## create an installable package
	@mkdir -p output
	bun pm pack --destination output
lint: ## check code and types, the browser entries without Node.js typings
	bun run lint
test: ## exercise the library against native files and loopback HTTP
	bun test tests
coverage: ## measure test coverage on the TypeScript sources
	bun test --coverage tests
e2e: pack ## install the package, kill/restart its workers and authority, run it in three browser engines
	node scripts/e2e.ts
integration: ## exercise authority contention and the real HTTP worker path
	bun test tests -t "100 competing pulls|worker over real HTTP"
bench: build ## measure durable worker contention
	node scripts/bench.ts
metrics: ## print the most recent benchmark record
	@cat output/bench.json
verify: lint test coverage e2e ## run release verification
start stop restart status log:
	@echo "Embedded library. The host application owns process lifecycle."
