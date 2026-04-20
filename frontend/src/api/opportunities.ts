import type {
  OpportunityDetailResponse,
  OpportunityHistoryResponse,
  OpportunityListResponse,
  OpportunityMaterializeResponse
} from "../appTypes";
import { apiGet, apiPost } from "./client";
import { buildOpportunityQuery } from "./defaults";

export async function fetchOpportunityHistory(): Promise<OpportunityHistoryResponse> {
  const query = await buildOpportunityQuery({ recentLimit: 6 });
  return apiGet<OpportunityHistoryResponse>("/api/v1/admin/models/opportunities/history", {
    errorPrefix: "Failed to load opportunity history",
    query
  });
}

export async function fetchOpportunities(): Promise<OpportunityListResponse> {
  const query = await buildOpportunityQuery({ includeLimit: true });
  return apiGet<OpportunityListResponse>("/api/v1/analyst/opportunities", {
    errorPrefix: "Failed to load opportunities",
    query
  });
}

export async function fetchOpportunityDetail(
  opportunityId: number
): Promise<OpportunityDetailResponse> {
  const query = await buildOpportunityQuery();
  return apiGet<OpportunityDetailResponse>(`/api/v1/analyst/opportunities/${opportunityId}`, {
    errorPrefix: "Failed to load opportunity detail",
    query
  });
}

export async function materializeOpportunities(): Promise<OpportunityMaterializeResponse> {
  const query = await buildOpportunityQuery();
  return apiPost<OpportunityMaterializeResponse>("/api/v1/admin/models/opportunities/materialize", {
    errorPrefix: "Failed to materialize opportunities",
    query
  });
}
