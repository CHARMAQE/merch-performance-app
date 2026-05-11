const API_BASE = process.env.REACT_APP_API_BASE || "http://localhost:9000";

async function requestDashboard(path, errorMessage) {
  const response = await fetch(`${API_BASE}${path}`);

  if (!response.ok) {
    throw new Error(errorMessage);
  }

  return response.json();
}

export function fetchDashboardOverview() {
  return fetchDashboardOverviewForStore();
}

export function fetchDashboardOverviewForStore(storeCode) {
  const params = new URLSearchParams();
  if (storeCode) {
    params.set("storeCode", storeCode);
  }

  const query = params.toString();
  return requestDashboard(
    `/api/dashboard/overview${query ? `?${query}` : ""}`,
    "Unable to load validation overview."
  );
}

export function fetchLatestValidationIssues(limit = 12, storeCode) {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  if (storeCode) {
    params.set("storeCode", storeCode);
  }

  return requestDashboard(
    `/api/dashboard/latest-issues?${params.toString()}`,
    "Unable to load validation issues."
  );
}
