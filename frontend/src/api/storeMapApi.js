const API_BASE = process.env.REACT_APP_API_BASE || "http://localhost:9000";

export async function fetchStoreMarkers() {
  const response = await fetch(`${API_BASE}/api/map/stores`);

  if (!response.ok) {
    throw new Error("Unable to load store markers.");
  }

  return response.json();
}

export async function fetchStoreDetails(storeCode) {
  const response = await fetch(
    `${API_BASE}/api/map/stores/${encodeURIComponent(storeCode)}/details`
  );

  if (!response.ok) {
    throw new Error("Unable to load store details.");
  }

  return response.json();
}
