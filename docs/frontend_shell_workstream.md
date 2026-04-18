# Frontend Shell Workstream

## Slice 1: API Module Extraction and Explicit Mode Defaults

This slice starts the frontend shell/API-client workstream by splitting the old [frontend/src/appApi.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appApi.ts) hotspot into focused API modules and making demo-versus-operator defaults explicit.

Implementation notes:
- New API modules now live under `frontend/src/api/`:
  - [frontend/src/api/client.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\api\client.ts)
  - [frontend/src/api/mode.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\api\mode.ts)
  - [frontend/src/api/backtests.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\api\backtests.ts)
  - [frontend/src/api/opportunities.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\api\opportunities.ts)
  - [frontend/src/api/models.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\api\models.ts)
  - [frontend/src/api/index.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\api\index.ts)
- [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx) now imports from the new API boundary directly.
- [frontend/src/appApi.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appApi.ts) remains as a compatibility re-export shim so the rest of the frontend can migrate incrementally.
- [frontend/src/vite-env.d.ts](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\vite-env.d.ts) now types the frontend mode and explicit demo/default env vars.

Behavior changes in this slice:
- Operator mode no longer injects hidden analyst/opportunity scope values such as:
  - `team_code=LAL`
  - `season_label=2024-2025`
  - `canonical_game_id=3`
- Demo defaults are now only applied when `VITE_APP_MODE=demo`.
- Future scoring-run detail loading no longer falls back to hardcoded `LAL`, `BOS`, or fixed dates in operator mode. Missing scenario identity now fails explicitly unless demo mode provides explicit demo defaults.

Why this matters:
- Transport code is now organized by workflow area instead of one catch-all file.
- Demo behavior is explicit and mode-gated instead of silently embedded in every fetch helper.
- The app can keep moving incrementally toward a thinner route shell without rewriting the whole UI in one patch.

Next recommended frontend slice:
1. Extract the backtest workspace from [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx) into a page-level component or hook boundary.
2. Do the same for the opportunity workspace, leaving `App.tsx` as route composition plus top-level shared state only.

## Slice 2: Backtests Workspace Boundary

This slice continues the frontend shell workstream by moving the main backtests route workspace into [frontend/src/backtestsWorkspace.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\backtestsWorkspace.tsx).

Implementation notes:
- [frontend/src/backtestsWorkspace.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\backtestsWorkspace.tsx) now owns:
  - the backtests stat grid
  - the dashboard/history layout for the backtests overview route
  - the strategy-results and walk-forward-fold sections for active runs
- [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx) now composes that workspace through a single `BacktestsWorkspace` boundary and passes only the route-specific detail card content for fold/run/artifact drill-down routes.
- The existing detail/provenance cards remain in `App.tsx` for now, which keeps this slice behavior-neutral while still shrinking the route shell and establishing a real page-level boundary.

Why this matters:
- `App.tsx` now owns less route-layout code for the backtests surface.
- The backtests overview and strategy sections can evolve without threading more conditional JSX through the shell.
- This creates a clean follow-up seam for moving the remaining backtest detail cards or applying the same extraction pattern to opportunities.

Next recommended frontend slice:
1. Extract the opportunity workspace from [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx) into its own page/workspace component.
2. Follow with a smaller cleanup slice that either moves the remaining backtest detail cards into the backtests module or promotes shared artifact/detail cards into a dedicated shared module.

## Slice 3: Opportunities Workspace Boundary

This slice applies the same shell-thinning pattern to the opportunity side by moving the main opportunity workspace into [frontend/src/opportunitiesWorkspace.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\opportunitiesWorkspace.tsx).

Implementation notes:
- [frontend/src/opportunitiesWorkspace.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\opportunitiesWorkspace.tsx) now owns:
  - the opportunity stat grid
  - the analyst queue layout and queue list items
  - the shared history-rollup section
- [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx) now composes that workspace through a single `OpportunitiesWorkspace` boundary and only supplies the route-specific detail content for opportunity, comparable-case, and artifact drill-down routes.
- The deeper detail/provenance cards remain in `App.tsx` for now so this slice stays behavior-neutral while removing another large route-layout branch from the shell.

Why this matters:
- `App.tsx` is now much closer to a real route shell instead of a monolithic page implementation.
- Queue presentation logic and rollup presentation logic can change without adding more conditional JSX to the shell.
- The next cleanup slice can focus on card/module ownership instead of still fighting top-level route layout.

Next recommended frontend slice:
1. Move the remaining backtest and opportunity detail/artifact cards out of [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx) into shared detail modules.
2. After that, collapse `App.tsx` further into route parsing, data loading, hero/toolbar composition, and workspace selection only.

## Slice 4: Shared Artifact Detail Modules

This slice starts the detail-card cleanup by moving the shared artifact/detail route surface into dedicated modules:
- [frontend/src/appArtifactDetailComponents.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appArtifactDetailComponents.tsx)
- [frontend/src/appOpportunityDetailComponents.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appOpportunityDetailComponents.tsx)

Implementation notes:
- `App.tsx` now renders the active opportunity detail, comparable-case detail, model-run detail, selection detail, evaluation detail, scoring-run detail, and artifact-compare route surfaces through imported shared components instead of relying on the inline definitions for those live paths.
- The in-file artifact detail copies for model-run, selection, evaluation, and scoring-run views were removed from [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx).
- This keeps behavior stable while moving the main detail-route ownership out of the shell and into dedicated modules.

Why this matters:
- The route shell is no longer the primary home of the artifact detail UI.
- Shared detail cards can now evolve independently of the top-level app shell and workspace boundaries.
- The remaining cleanup is narrower: mostly the backtest-specific detail cards plus removal of the last legacy in-file copies.

Next recommended frontend slice:
1. Remove the remaining legacy in-file opportunity/backtest detail copies from [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx), especially the last backtest-specific detail cards and compare card.
2. After that, leave `App.tsx` focused on route parsing, data loading, hero/toolbar composition, and workspace selection only.

## Slice 5: Backtest Detail Boundary

This slice moves the last live backtest-specific detail routes behind a dedicated module in [frontend/src/appBacktestDetailComponents.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appBacktestDetailComponents.tsx).

Implementation notes:
- `App.tsx` now renders the backtest run detail and fold detail routes through imported shared components instead of owning the live route implementation directly.
- The shell now composes three dedicated detail/component boundaries:
  - [frontend/src/appBacktestDetailComponents.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appBacktestDetailComponents.tsx)
  - [frontend/src/appArtifactDetailComponents.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appArtifactDetailComponents.tsx)
  - [frontend/src/appOpportunityDetailComponents.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\appOpportunityDetailComponents.tsx)
- There is still a final cleanup pass left to delete the last legacy in-file copies that are no longer on the live route path, but the runtime ownership is now moved out of the shell.

Why this matters:
- `App.tsx` is now routing through dedicated backtest, opportunity, and artifact-detail modules instead of being the primary implementation surface for those routes.
- The remaining cleanup is mechanical dead-code removal rather than another architectural extraction.

Next recommended frontend slice:
1. Remove the last unused legacy detail/component copies still defined in [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx).
2. Do a final shell tidy-up pass on imports/types so `App.tsx` reads as a route shell rather than a mixed shell-plus-legacy file.

## Slice 6: App Shell Closeout

This slice finishes the shell-thinning pass by removing the final dead legacy detail block from [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx) and trimming the imports it had been keeping alive.

Implementation notes:
- The last stale in-file fold detail implementation was removed from [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx).
- `App.tsx` now reads as a route shell that focuses on:
  - hash-route parsing
  - top-level data loading
  - hero and mode-toolbar composition
  - workspace selection
  - detail-route wiring into the shared modules
- Dead imports for legacy stat/detail helpers were removed so the shell depends only on the route-level state and provenance data it still owns.

Why this matters:
- The main app entry is no longer a mixed shell-plus-legacy component file.
- UI ownership is now much clearer: workspaces and detail cards live in dedicated modules, while `App.tsx` coordinates navigation and data flow.
- The next frontend slices can focus on optional hook extraction or route-system upgrades instead of more dead-code cleanup.

Next recommended frontend slice:
1. Extract the top-level data-loading and detail-fetch effects from [frontend/src/App.tsx](C:\Users\Ivica\Downloads\bookmakers-mistake-detector\frontend\src\App.tsx) into an app-shell hook if we want to keep shrinking the entry component.
2. Or stop here and treat the current shell boundary as the Phase 5 frontend exit point, since the major structural split is complete.
