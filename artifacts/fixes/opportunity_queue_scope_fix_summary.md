# Opportunity Queue Scope Refresh Fix

## Files changed

- `backend/src/bookmaker_detector_api/services/model_records.py`
- `backend/src/bookmaker_detector_api/services/model_opportunities.py`
- `backend/src/bookmaker_detector_api/services/model_market_board_views.py`
- `backend/src/bookmaker_detector_api/services/models.py`
- `backend/src/bookmaker_detector_api/api/analyst_opportunities.py`
- `backend/src/bookmaker_detector_api/api/schemas/analyst.py`
- `backend/src/bookmaker_detector_api/repositories/postgres_migration_authority.py`
- `infra/postgres/init/023_phase3_opportunity_materialization_batch_schema.sql`
- `backend/tests/test_models.py`
- `backend/tests/test_admin_routes.py`
- `frontend/src/App.tsx`
- `frontend/src/appTypes.ts`
- `frontend/src/opportunitiesWorkspace.tsx`
- `frontend/src/appOpportunityDetailComponents.tsx`
- `frontend/src/styles.css`
- `frontend/e2e/opportunity-queue-scope.spec.ts`

## Migration

- Added `023_phase3_opportunity_materialization_batch_schema.sql`.
- The migration adds first-class opportunity materialization provenance columns, backfills legacy rows as `legacy`, drops the old single-column uniqueness on `opportunity_key`, and replaces it with batch-aware uniqueness on `(materialization_batch_id, opportunity_key)`.

## Tests added or updated

- `backend/tests/test_models.py`
  - scope provenance persists on materialized opportunities
  - latest relevant batch selection prefers operator-wide vs scoped batches correctly
- `backend/tests/test_admin_routes.py`
  - analyst opportunity list/detail routes expose queue metadata and stump explainability
- `frontend/e2e/opportunity-queue-scope.spec.ts`
  - scoped queue warning and operator-wide labeling
  - tree stump explanation visibility

## Verification run

- `pytest backend/tests/test_models.py -k "opportunity"`
- `pytest backend/tests/test_admin_routes.py -k "phase_three_model_opportun"`
- `npm run typecheck` in `frontend`
- `npm run build` in `frontend`

## Manual verification

1. Materialize a team-scoped queue such as `team_code=LAL`.
2. Open the Opportunities workspace and confirm it shows a scoped badge, scope label, and warning copy.
3. Materialize an unscoped operator queue.
4. Re-open the Opportunities workspace and confirm the queue label switches to operator-wide.
5. Open an opportunity detail and confirm the tree stump explanation panel shows selected feature, threshold, leaf predictions, and branch.

## Notes

- The new Playwright regression spec was added, but it could not be executed in this Windows environment because Node fails before Playwright startup with `EPERM: operation not permitted, lstat 'C:\\Users\\Ivica'`.
