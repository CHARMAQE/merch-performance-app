const API_BASE =
  process.env.REACT_APP_API_BASE_URL || "http://localhost:9000";

const PROBLEMATIC_STORES_PATH =
  "/api/backoffice/store-map/problematic-stores";

export async function fetchStoreMarkers() {
  const response = await fetch(`${API_BASE}/api/map/stores`);

  if (!response.ok) {
    throw new Error("Unable to load store markers.");
  }

  return response.json();
}

function appendParam(params, key, value) {
  if (value !== undefined && value !== null && String(value).trim()) {
    params.set(key, String(value).trim());
  }
}

export async function getProblematicStores(filters = {}) {
  const params = new URLSearchParams();
  appendParam(params, "channel", filters.channel);
  appendParam(params, "startDate", filters.startDate);
  appendParam(params, "endDate", filters.endDate);
  appendParam(params, "storeCode", filters.storeCode);
  appendParam(params, "employeeCode", filters.employeeCode);
  if (filters.limit !== undefined && filters.limit !== null) {
    params.set("limit", String(filters.limit));
  }

  const query = params.toString();
  const url = `${API_BASE}${PROBLEMATIC_STORES_PATH}${
    query ? `?${query}` : ""
  }`;

  const response = await fetch(url);

  if (!response.ok) {
    throw new Error("Unable to load problematic stores.");
  }

  const data = await response.json();
  return data;
}

export const fetchProblematicStores = getProblematicStores;

export async function fetchStoreDetails(storeCode) {
  const response = await fetch(
    `${API_BASE}/api/map/stores/${encodeURIComponent(storeCode)}/details`
  );

  if (!response.ok) {
    throw new Error("Unable to load store details.");
  }

  return response.json();
}
