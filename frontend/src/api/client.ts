export const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

function buildUrl(path: string, query?: URLSearchParams): string {
  const queryString = query?.toString();
  return queryString ? `${apiBaseUrl}${path}?${queryString}` : `${apiBaseUrl}${path}`;
}

async function parseJsonResponse<T>(response: Response, errorPrefix: string): Promise<T> {
  if (!response.ok) {
    throw new Error(`${errorPrefix} (${response.status})`);
  }
  return (await response.json()) as T;
}

export async function apiGet<T>(
  path: string,
  options?: { query?: URLSearchParams; errorPrefix?: string }
): Promise<T> {
  const response = await fetch(buildUrl(path, options?.query));
  return parseJsonResponse<T>(response, options?.errorPrefix ?? `Failed to load ${path}`);
}

export async function apiPost<T>(
  path: string,
  options?: { query?: URLSearchParams; errorPrefix?: string }
): Promise<T> {
  const response = await fetch(buildUrl(path, options?.query), { method: "POST" });
  return parseJsonResponse<T>(response, options?.errorPrefix ?? `Failed to post ${path}`);
}
