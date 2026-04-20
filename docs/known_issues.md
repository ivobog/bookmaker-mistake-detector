# Known Issues

## Current Status
Phase 5 closeout remains in progress, but the previously open worker startup, backend validation, and Playwright smoke issues are now closed. The remaining item below is a mitigated Docker Compose reliability note.

## Tracking Guidance
Capture issues here when they affect any of the following:
- regression script stability
- Docker startup reliability
- ingestion correctness
- external source refresh behavior
- market-board cadence flow
- model selection or scoring provenance
- frontend drill-through navigation
- compare-route analyst decisions

## Suggested Issue Template
Use this format for each new item:

### Title
- Severity: `blocker` | `high` | `medium` | `low`
- Area: backend | worker | frontend | infra | docs
- Status: open | mitigated | closed
- Summary:
- Reproduction:
- Temporary mitigation:
- Exit criteria:

### Docker compose stale container conflict after interrupted rebuild
- Severity: `medium`
- Area: `infra`
- Status: `mitigated`
- Summary: During the first Phase 5 smoke pass, `docker compose up -d --build` failed because a stale created backend container name remained allocated after an interrupted or partial prior Compose run.
- Reproduction: Run a Compose build/start sequence, interrupt or leave it in a partial state, then rerun `docker compose up -d --build` and observe a backend container-name conflict.
- Temporary mitigation: Run `docker compose down --remove-orphans`, inspect `docker ps -a`, remove stale created containers if needed, and rerun the stack bring-up.
- Exit criteria: Repeated release-candidate startup passes should succeed without manual stale-container cleanup.

### Worker startup fails after the Postgres-only cleanup
- Severity: `blocker`
- Area: `worker`
- Status: `closed`
- Summary: [main.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/worker/src/bookmaker_detector_worker/main.py) now uses the supported Postgres-era ingestion and fixture-loading helpers instead of importing the retired ingestion-runner modules, and the worker stays up in Docker Compose with the current stack.
- Reproduction: Previously reproduced by running `docker compose up --build` and observing `bookmaker-worker` exit with `ModuleNotFoundError` for the removed runner modules.
- Temporary mitigation: None required after the worker cutover. The worker now defaults to `idle` when no supported job mode is configured.
- Exit criteria: Satisfied. `python -m py_compile worker/src/bookmaker_detector_worker/main.py` passes, and `docker compose ps -a` shows `bookmaker-worker` staying `Up` with the current stack.

### Analyst comparables and evidence return 500 when `canonical_game_id` is used without `team_code`
- Severity: `medium`
- Area: `backend`
- Status: `closed`
- Summary: [analyst_patterns.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/src/bookmaker_detector_api/api/analyst_patterns.py) now translates the comparable-anchor perspective error into a `400` response with a clear validation message instead of leaking it as a `500`.
- Reproduction: Previously reproduced by calling `GET /api/v1/analyst/comparables?target_task=spread_error_regression&canonical_game_id=1` or `GET /api/v1/analyst/evidence?target_task=spread_error_regression&canonical_game_id=1` without `team_code` when the canonical game had multiple perspectives.
- Temporary mitigation: None required after the route-level validation translation.
- Exit criteria: Satisfied. [test_admin_routes.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/backend/tests/test_admin_routes.py) now covers both routes and verifies they return `400` with the explicit `team_code` guidance.

### Playwright Phase 5 mutation smoke no longer matches current model-admin behavior
- Severity: `medium`
- Area: `frontend`
- Status: `closed`
- Summary: [phase5-smoke.spec.ts](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend/e2e/phase5-smoke.spec.ts) now matches the intentional model-admin UX by stubbing capabilities, using the canonical selection policy name, and asserting the durable post-mutation detail-route outcome instead of a transient banner.
- Reproduction: Previously reproduced by running `cmd /c npm run test:smoke` in [frontend](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend) and observing the `runs train and select mutations with browser-level refresh verification` smoke fail while waiting for a banner that disappears during route remount.
- Temporary mitigation: None required after the smoke realignment.
- Exit criteria: Satisfied. `cmd /c npm run test:smoke` now completes with `5 passed`.
