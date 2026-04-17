# Release Candidate Runbook

## Purpose
This runbook is the Phase 5 starting point for validating the MVP as a release candidate.

It focuses on the workflows that now span:
- historical ingestion
- analytical feature generation
- predictive model selection
- market-board refresh and scoring cadence
- Phase 4 analyst backtest and opportunity review UX

Use it together with:
- [docs/release_acceptance_checklist.md](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/docs/release_acceptance_checklist.md)
- [docs/manual_smoke_checklist.md](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/docs/manual_smoke_checklist.md)
- [docs/known_issues.md](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/docs/known_issues.md)

## Schema Contract
- PostgreSQL schema ownership lives in `infra/postgres/init/`.
- Normal API and worker execution must assume the schema already exists.
- Runtime DDL is no longer part of request handling or worker execution.
- `docker compose up --build` applies the init SQL only when the Postgres data directory is empty.
- If you point the stack at an existing database or reused volume, apply the SQL in `infra/postgres/init/` before starting postgres-backed services.
- Production backend startup now fails fast when required tables are missing, with an error that points back to `infra/postgres/init`.

## Default Regression Pass
From the repository root, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_phase5_regression.ps1
```

What it covers:
- backend Ruff checks
- backend pytest suite
- Python compile checks for backend and worker
- frontend typecheck
- frontend lint
- frontend production build

Optional browser-route smoke:

```powershell
cd .\frontend
npm run test:smoke
```

This Playwright smoke pass is intended for the Phase 4 hash-routed analyst workflow. It is useful for
release-candidate validation, but it may be explicitly waived if the operator chooses to skip browser
execution for a given pass.

Initial production dataset bootstrap:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_initial_production_dataset_load.ps1 -SourceUrlTemplate "<provider-template>"
```

The loader reads the team and season scope from the reference tables by default, uses the last four
completed seasons when no explicit season filter is provided, and records one fetch-and-ingest job
per team-season target in Postgres.

Schema preflight before production-oriented validation:
- confirm `infra/postgres/init/` matches the database you are about to use
- if you are reusing a Docker volume or external Postgres instance, apply the SQL manually before boot
- start the backend and verify startup succeeds before running ingestion, scoring, or bootstrap flows
- treat a schema-readiness startup failure as an environment/setup issue, not as an application bug in the feature/model routes

## Acceptance Sequence
Run Phase 5 in this order:
1. Execute the regression script.
2. Complete the manual smoke checklist.
3. Record blockers or waivers in the known-issues file.
4. Make the release recommendation from the acceptance checklist.

## Suggested Manual Smoke Pass
After the regression script passes:

1. Start the stack with `docker compose up --build`.
2. Open the frontend at `http://localhost:5173`.
3. Confirm the API health route at `http://localhost:8000/api/v1/health`.
4. Run the Phase 1 demo route.
5. Run one Phase 3 market-board refresh and one cadence/orchestration flow.
6. Run one Phase 4 backtest from the frontend.
7. Open:
   - one backtest run
   - one fold
   - one opportunity
   - one comparable case
   - one compare route
8. Confirm the compare route shows:
   - alignment summary
   - mismatch review
   - decision summary
9. If you want browser-backed route verification instead of manual clicking, run `npm run test:smoke`
   from [frontend](C:/Users/Ivica/Downloads/bookmakers-mistake-detector/frontend) after the stack is up.

As you complete the smoke pass:
- record pass/fail in `docs/manual_smoke_checklist.md`
- move blockers into `docs/known_issues.md`
- update the release decision section in `docs/release_acceptance_checklist.md`

## External Source Readiness
Before validating the live external source path:

- set `THE_ODDS_API_KEY`
- verify `THE_ODDS_API_*` values in `.env`
- confirm backend starts cleanly with those values present or absent

If no live key is available, treat the file-backed source and demo providers as the required MVP fallback.

## Release Gate Notes
The release candidate should not be marked ready until:
- regression script is green
- manual smoke pass is green or explicitly waived with documented rationale
- acceptance checklist is reviewed against the SRS
- no blocking ingestion, scoring, or UI navigation issues remain
- known issues are captured in `docs/known_issues.md`
