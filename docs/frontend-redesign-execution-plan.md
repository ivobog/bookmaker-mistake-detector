# Frontend Redesign Execution Plan

## Goal
Create a completely new workflow-first frontend project for Bookmaker Mistake Detector that follows the UX blueprint in `C:/Users/Ivica/Downloads/bookmaker_mistake_detector_ux_redesign_blueprint.docx` without disturbing the current production-facing `frontend` app.

## Delivery approach
- Build the redesign as a separate application in `frontend-redesign/`.
- Preserve the current `frontend/` workspace as the fallback and contract reference.
- Start with a standalone shell that can render with mock data, then hydrate from live API endpoints when available.
- Reorganize the product around user intent:
  - `Home`
  - `Training Lab`
  - `Model Decision`
  - `Slate Runner`
  - `Signals Desk`

## Product rules from the blueprint
- Navigation must reflect user jobs, not backend artifacts.
- The workflow must always show what is ready, stale, blocked, or next.
- Primary actions should use plain verbs.
- Parameters should be grouped into essentials, expert settings, and system defaults.
- Every major action should end with a result summary and a recommended next step.

## Project structure
- `frontend-redesign/`
  - new Vite + React + TypeScript app
  - dedicated styles and component shell
  - internal route model based on workflow destinations
  - live-or-mock data adapter layer
- `docs/frontend-redesign-execution-plan.md`
  - redesign execution reference

## Phases

### Phase 1: Foundation
- Scaffold the new project with its own `package.json`, Vite config, TS config, and app entrypoint.
- Define the redesign information architecture and internal page model.
- Establish shared visual tokens, layout primitives, and workflow-status components.

### Phase 2: Workflow shell
- Implement the five primary destinations and the three secondary destinations:
  - `History`
  - `Settings`
  - `Help`
- Add the persistent workflow strip and next-action callout.
- Build page sections around cards, split layouts, and analyst-friendly evidence blocks instead of console-style route lists.

### Phase 3: Data adapter
- Create a live data loader that reads current backend endpoints when possible:
  - model capabilities
  - model summary/history
  - selection history
  - backtest history
  - opportunities history/list
- Keep a mock dataset as the resilience layer so the redesign remains demoable even if the backend is unavailable.
- Normalize backend payloads into a workflow-centric app model.

### Phase 4: Interaction polish
- Add search and filtering for Signals Desk.
- Surface presets and parameter tiers in Training Lab and Slate Runner.
- Highlight active, stale, and review-needed states with consistent status semantics.

### Phase 5: Integration hardening
- Install dependencies for the new workspace.
- Run typecheck, lint, and build for `frontend-redesign/`.
- Decide whether the redesign should stay parallel, replace `frontend/`, or be mounted behind a feature flag.

## Current implementation scope
This first implementation pass covers:
- greenfield project scaffold
- workflow navigation
- visual system
- page composition for the new IA
- live-or-mock data flow

This pass intentionally does not yet cover:
- full mutation wiring for training, selection, slate execution, and signal persistence
- replacement of existing Playwright coverage
- production cutover of the current `frontend/` app

## Acceptance criteria
- A teammate can run `frontend-redesign/` independently from the current app.
- The new app renders the five blueprint destinations with a clear workflow strip.
- The new app communicates status, next step, and parameter tiers in plain language.
- The new app can show real backend-derived status when the API is reachable.
- The current `frontend/` code remains untouched.

## Recommended next steps after this scaffold
1. Wire mutations for `Train new run`, `Activate model`, `Run slate`, and `Save signals`.
2. Add detail drawers for training history, validation evidence, and signal provenance.
3. Introduce route-level tests for workflow strip status resolution and Signals Desk filtering.
4. Decide cutover strategy: parallel preview, feature flag, or replacement.
