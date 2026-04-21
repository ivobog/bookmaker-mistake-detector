import { useEffect, useMemo, useRef } from "react";
import { createRoot, type Root } from "react-dom/client";

import WorkflowDeskApp, { type RoutingAdapter } from "../../frontend-redesign/src/App";
import workflowDeskStyles from "../../frontend-redesign/src/styles.css?inline";

function normalizeDeskPath(value: string | undefined): string {
  return value?.replace(/^#\/?/, "").replace(/^\/+/, "") || "home";
}

export function WorkflowDeskBridge({
  deskPath,
  enabled,
  onNavigateDeskPath
}: {
  deskPath?: string;
  enabled: boolean;
  onNavigateDeskPath: (nextDeskPath: string) => void;
}) {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const rootRef = useRef<Root | null>(null);
  const mountNodeRef = useRef<HTMLDivElement | null>(null);
  const normalizedDeskPath = normalizeDeskPath(deskPath);
  const routingAdapter = useMemo<RoutingAdapter>(
    () => ({
      getHash: () => `#/${normalizedDeskPath}`,
      pushHash: (nextHash) => {
        onNavigateDeskPath(normalizeDeskPath(nextHash));
      },
      replaceHash: (nextHash) => {
        onNavigateDeskPath(normalizeDeskPath(nextHash));
      },
      subscribe: () => () => undefined
    }),
    [normalizedDeskPath, onNavigateDeskPath]
  );

  useEffect(() => {
    if (!enabled || !hostRef.current) {
      return;
    }

    const shadowRoot = hostRef.current.shadowRoot ?? hostRef.current.attachShadow({ mode: "open" });
    if (!mountNodeRef.current) {
      const styleElement = document.createElement("style");
      styleElement.textContent = workflowDeskStyles;
      shadowRoot.appendChild(styleElement);

      const mountNode = document.createElement("div");
      mountNode.dataset.workflowDeskRoot = "true";
      shadowRoot.appendChild(mountNode);

      mountNodeRef.current = mountNode;
      rootRef.current = createRoot(mountNode);
    }

    return () => {
      rootRef.current?.unmount();
      rootRef.current = null;
      mountNodeRef.current = null;
      shadowRoot.textContent = "";
    };
  }, [enabled]);

  useEffect(() => {
    if (!enabled || !rootRef.current) {
      return;
    }

    rootRef.current.render(<WorkflowDeskApp key={normalizedDeskPath} routingAdapter={routingAdapter} />);
  }, [enabled, normalizedDeskPath, routingAdapter]);

  if (!enabled) {
    return (
      <section className="workflow-desk-bridge-shell">
        <article className="workflow-desk-bridge-card">
          <p className="eyebrow">Workflow Desk</p>
          <h2>Feature flag required</h2>
          <p className="lead">
            Enable <code>VITE_ENABLE_WORKFLOW_DESK=true</code> in the existing frontend to open the redesign inside this
            shell.
          </p>
        </article>
      </section>
    );
  }

  return (
    <section className="workflow-desk-bridge-shell">
      <div className="workflow-desk-host" ref={hostRef} />
    </section>
  );
}
