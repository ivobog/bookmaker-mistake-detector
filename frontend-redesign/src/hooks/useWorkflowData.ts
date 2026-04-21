import { useEffect, useState } from "react";

import { loadLiveAppData } from "../data/liveApi";
import { buildMockAppData } from "../data/mockData";
import type { WorkflowDataState } from "../types";

export function useWorkflowData(): WorkflowDataState {
  const [state, setState] = useState<WorkflowDataState>(() => ({
    data: buildMockAppData(),
    loading: true,
    warning: null,
    reload: async () => {}
  }));

  useEffect(() => {
    let cancelled = false;

    async function load() {
      const liveData = await loadLiveAppData();
      if (cancelled) {
        return;
      }

      if (liveData) {
        setState((current) => ({
          ...current,
          data: liveData,
          loading: false,
          warning: null
        }));
        return;
      }

      setState((current) => ({
        ...current,
        data: buildMockAppData(),
        loading: false,
        warning: "Live API was unavailable, so the redesign is showing its built-in prototype dataset."
      }));
    }

    const reload = async () => {
      setState((current) => ({ ...current, loading: true }));
      await load();
    };

    setState((current) => ({ ...current, reload }));
    void load();

    return () => {
      cancelled = true;
    };
  }, []);

  return state;
}
