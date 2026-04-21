# Frontend Redesign

Parallel redesign workspace for the Bookmaker Mistake Detector frontend.

## Commands
- `npm install`
- `npm run dev`
- `npm run typecheck`
- `npm run lint`
- `npm run build`

## Notes
- Runs on `5174` in dev so it does not collide with the current `frontend/` app on `5173`.
- Uses live API data when available through `VITE_API_BASE_URL`.
- Falls back to internal mock data so the redesign shell remains explorable without backend availability.
