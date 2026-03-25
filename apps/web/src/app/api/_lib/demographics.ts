const NSE_GROUPS: Record<string, string[]> = {
  AB: ["AB", "A", "B"],
  C: ["C+", "C", "C-"],
  DE: ["D+", "D", "DE", "E"],
};

function asList(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value
      .map((item) => (typeof item === "string" ? item.trim() : ""))
      .filter(Boolean);
  }
  if (typeof value === "string") {
    return value
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
  }
  return [];
}

function normalizeNseToken(value: string) {
  return value.trim().toUpperCase().replace(/\s+/g, "");
}

export function expandNseFilterValues(value: unknown): string[] {
  const list = asList(value);
  const expanded = new Set<string>();
  for (const item of list) {
    const token = normalizeNseToken(item);
    const mapped = NSE_GROUPS[token];
    if (mapped) {
      for (const raw of mapped) expanded.add(raw);
      continue;
    }
    expanded.add(item.trim());
  }
  return Array.from(expanded);
}

export function collapseNseOptions(value: unknown): string[] {
  const list = asList(value);
  const normalizedSet = new Set(list.map(normalizeNseToken));
  const grouped: string[] = [];

  if (NSE_GROUPS.AB.some((item) => normalizedSet.has(normalizeNseToken(item)))) grouped.push("AB");
  if (NSE_GROUPS.C.some((item) => normalizedSet.has(normalizeNseToken(item)))) grouped.push("C");
  if (NSE_GROUPS.DE.some((item) => normalizedSet.has(normalizeNseToken(item)))) grouped.push("DE");

  return grouped;
}

export function expandNseInPayload(payload: Record<string, unknown>): Record<string, unknown> {
  if (!Object.prototype.hasOwnProperty.call(payload, "nse")) return payload;
  return {
    ...payload,
    nse: expandNseFilterValues(payload.nse),
  };
}

export function expandNseInQuery(query: Record<string, string>): Record<string, string> {
  if (!query.nse) return query;
  const expanded = expandNseFilterValues(query.nse);
  return {
    ...query,
    nse: expanded.join(","),
  };
}

