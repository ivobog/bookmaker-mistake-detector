const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

const phaseZeroChecklist = [
  "Backend API scaffold",
  "Worker scaffold",
  "Postgres init schema",
  "Docker Compose local stack",
  "CI baseline"
];

export default function App() {
  return (
    <main className="app-shell">
      <section className="hero">
        <p className="eyebrow">Bookmaker Mistake Detector</p>
        <h1>Phase 0 foundation is in place.</h1>
        <p className="lead">
          This starter UI gives us a clean base for the analyst console while the data spine
          and intelligence layers come online in later phases.
        </p>
      </section>

      <section className="status-grid">
        <article className="panel">
          <h2>Local services</h2>
          <p>Frontend, API, worker, and Postgres are wired for local development.</p>
          <code>{apiBaseUrl}/api/v1/health</code>
        </article>

        <article className="panel">
          <h2>Phase 0 checklist</h2>
          <ul>
            {phaseZeroChecklist.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </article>
      </section>
    </main>
  );
}

