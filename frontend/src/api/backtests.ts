import type {
  BacktestHistoryResponse,
  BacktestRunResponse
} from "../appTypes";
import { apiGet, apiPost } from "./client";
import { buildBacktestQuery } from "./defaults";

export async function fetchBacktestHistory(): Promise<BacktestHistoryResponse> {
  const query = await buildBacktestQuery();
  query.set("recent_limit", "6");
  return apiGet<BacktestHistoryResponse>("/api/v1/admin/models/backtests/history", {
    errorPrefix: "Failed to load backtest history",
    query
  });
}

export async function runBacktest(): Promise<BacktestRunResponse> {
  const query = await buildBacktestQuery();
  return apiPost<BacktestRunResponse>("/api/v1/admin/models/backtests/run", {
    errorPrefix: "Failed to run backtest",
    query
  });
}

export async function fetchBacktestRunDetail(backtestRunId: number): Promise<BacktestRunResponse> {
  const query = await buildBacktestQuery();
  return apiGet<BacktestRunResponse>(`/api/v1/analyst/backtests/${backtestRunId}`, {
    errorPrefix: "Failed to load backtest run",
    query
  });
}
