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

async function fetchWithTimeout(
  input: string,
  init?: RequestInit,
  timeoutMs?: number
): Promise<Response> {
  if (!timeoutMs || timeoutMs <= 0) {
    return fetch(input, init);
  }

  const controller = new AbortController();
  const timeoutHandle = window.setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(input, { ...init, signal: controller.signal });
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new Error(`Request timed out after ${Math.round(timeoutMs / 1000)} seconds.`);
    }
    throw error;
  } finally {
    window.clearTimeout(timeoutHandle);
  }
}

export async function apiGet<T>(
  path: string,
  options?: { query?: URLSearchParams; errorPrefix?: string; timeoutMs?: number }
): Promise<T> {
  const response = await fetchWithTimeout(buildUrl(path, options?.query), undefined, options?.timeoutMs);
  return parseJsonResponse<T>(response, options?.errorPrefix ?? `Failed to load ${path}`);
}

export async function apiPost<T>(
  path: string,
  options?: { query?: URLSearchParams; errorPrefix?: string; timeoutMs?: number }
): Promise<T> {
  const response = await fetchWithTimeout(
    buildUrl(path, options?.query),
    { method: "POST" },
    options?.timeoutMs
  );
  return parseJsonResponse<T>(response, options?.errorPrefix ?? `Failed to post ${path}`);
}

export async function fetchJson<T>(path: string, query?: URLSearchParams, errorPrefix?: string): Promise<T> {
  return apiGet<T>(path, {
    errorPrefix: errorPrefix ?? `Request failed: ${path}`,
    query
  });
}

export async function postJson<T>(path: string, query?: URLSearchParams, errorPrefix?: string): Promise<T> {
  return apiPost<T>(path, {
    errorPrefix: errorPrefix ?? `Request failed: ${path}`,
    query
  });
}
