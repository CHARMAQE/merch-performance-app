export function normalizeSearchText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

export function getSearchScore(query, values) {
  const normalizedQuery = normalizeSearchText(query);

  if (!normalizedQuery) {
    return 0;
  }

  const normalizedValues = values
    .map((value) => normalizeSearchText(value))
    .filter(Boolean);
  const haystack = normalizedValues.join(" ");
  const queryTerms = normalizedQuery.split(" ").filter(Boolean);

  if (!queryTerms.every((term) => haystack.includes(term))) {
    return 0;
  }

  if (normalizedValues.some((value) => value === normalizedQuery)) {
    return 120;
  }

  if (normalizedValues.some((value) => value.startsWith(normalizedQuery))) {
    return 100;
  }

  if (haystack.startsWith(normalizedQuery)) {
    return 80;
  }

  if (normalizedValues.some((value) => value.includes(normalizedQuery))) {
    return 60;
  }

  return 30 + queryTerms.length;
}

export function sortBySearchScore(items, query, valuesGetter) {
  return items
    .map((item) => ({
      item,
      score: getSearchScore(query, valuesGetter(item)),
    }))
    .filter((entry) => entry.score > 0)
    .sort((a, b) => b.score - a.score)
    .map((entry) => entry.item);
}
