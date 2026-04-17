# Known Issues

## Current Status
No Phase 5 release-blocking issues are recorded yet in this document.

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
