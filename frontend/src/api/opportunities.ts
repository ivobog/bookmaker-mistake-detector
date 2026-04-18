import type {
  OpportunityDetailResponse,
  OpportunityHistoryResponse,
  OpportunityListResponse,
  OpportunityMaterializeResponse
} from "../appTypes";
import { apiGet, apiPost } from "./client";
import { buildOpportunityQuery } from "./mode";

export async function fetchOpportunityHistory(): Promise<OpportunityHistoryResponse> {
  return apiGet<OpportunityHistoryResponse>("/api/v1/admin/models/opportunities/history", {
    errorPrefix: "Failed to load opportunity history",
    query: buildOpportunityQuery({ recentLimit: 6 })
  });
}

export async function fetchOpportunities(): Promise<OpportunityListResponse> {
  return apiGet<OpportunityListResponse>("/api/v1/analyst/opportunities", {
    errorPrefix: "Failed to load opportunities",
    query: buildOpportunityQuery({ includeLimit: true })
  });
}

export async function fetchOpportunityDetail(
  opportunityId: number
): Promise<OpportunityDetailResponse> {
  return apiGet<OpportunityDetailResponse>(`/api/v1/analyst/opportunities/${opportunityId}`, {
    errorPrefix: "Failed to load opportunity detail",
    query: buildOpportunityQuery()
  });
}

export async function materializeOpportunities(): Promise<OpportunityMaterializeResponse> {
  return apiPost<OpportunityMaterializeResponse>("/api/v1/admin/models/opportunities/materialize", {
    errorPrefix: "Failed to materialize opportunities",
    query: buildOpportunityQuery()
  });
}
