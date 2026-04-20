# Known Issues

## Current Status
Phase 5 closeout is in progress. Current open items are recorded below.

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
- Status: `open`
- Summary: The Docker Compose worker exits immediately because [main.py](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/worker/src/bookmaker_detector_worker/main.py) still imports `bookmaker_detector_api.services.fetch_ingestion_runner` and `bookmaker_detector_api.services.fixture_ingestion_runner`, both of which were intentionally removed during the Postgres-only backend cutover.
- Reproduction: Run `docker compose up --build` from the repo root with the current `.env`; `bookmaker-worker` exits with `ModuleNotFoundError` for the retired ingestion-runner modules.
- Temporary mitigation: Disable the worker for manual smoke, or update the worker to stop importing and dispatching the retired runner paths before relying on a full Compose bring-up.
- Exit criteria: `bookmaker-worker` must start cleanly with the supported job modes in the current codebase, or the Compose stack must stop advertising unsupported worker modes.

### Analyst comparables and evidence return 500 when `canonical_game_id` is used without `team_code`
- Severity: `medium`
- Area: `backend`
- Status: `open`
- Summary: The live analyst comparables and evidence routes require `team_code` when a canonical game has multiple perspectives, but the current API surfaces that requirement as a 500 instead of a client-validation response.
- Reproduction: Seed the Postgres-backed smoke dataset, then call `GET /api/v1/analyst/comparables?target_task=spread_error_regression&canonical_game_id=1` or `GET /api/v1/analyst/evidence?target_task=spread_error_regression&canonical_game_id=1` without `team_code`.
- Temporary mitigation: Provide `team_code` alongside `canonical_game_id` for analyst comparables/evidence calls until the route or service returns a proper 4xx validation response.
- Exit criteria: The API should either infer the correct perspective safely or return a clear 4xx validation error that explains the missing `team_code` requirement.

### Playwright Phase 5 mutation smoke no longer matches current model-admin behavior
- Severity: `medium`
- Area: `frontend`
- Status: `open`
- Summary: `npm run test:smoke` now finishes 4/5 passing, with the remaining failure in `frontend/e2e/phase5-smoke.spec.ts` where the train mutation verification still expects an immediate redirect to `#/models/runs/302`.
- Reproduction: Run `cmd /c npm run test:smoke` in [frontend](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend); the failing test is `Phase 5 browser smoke › runs train and select mutations with browser-level refresh verification`.
- Temporary mitigation: Use the passing analyst/model-admin route smoke coverage for browser navigation confidence, and manually verify the train/select mutation UX until the test is aligned with the current workspace behavior.
- Exit criteria: Either restore the intended post-mutation navigation behavior or update the Playwright assertion to match the current, intentional UX.
