export function formatJsonLikeValue(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "string") {
    return value;
  }

  if (typeof value !== "object") {
    return String(value);
  }

  if (Object.keys(value).length === 0) {
    return null;
  }

  return JSON.stringify(value, null, 2);
}

export function formatStoredRationaleValue(
  rationale: string | Record<string, unknown> | null,
  emptyFallback = "n/a"
): string {
  if (rationale === null) {
    return emptyFallback;
  }

  if (typeof rationale === "string") {
    return rationale;
  }

  const reason = rationale.reason;
  if (typeof reason === "string" && reason.trim()) {
    return reason;
  }

  return formatJsonLikeValue(rationale) ?? emptyFallback;
}
