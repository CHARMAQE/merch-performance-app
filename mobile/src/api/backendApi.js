const LAN_API_BASE = "http://192.168.0.124:9000";
const REQUEST_TIMEOUT_MS = 30000;

export const API_BASE = LAN_API_BASE;

async function request(path, options = {}) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  let response;
  try {
    response = await fetch(`${API_BASE}${path}`, {
      method: options.method || "GET",
      headers: {
        Accept: "application/json",
        ...(options.body ? { "Content-Type": "application/json" } : {}),
        ...(options.headers || {}),
      },
      body: options.body,
      signal: controller.signal,
    });
  } catch (error) {
    if (error.name === "AbortError") {
      throw new Error("Request timed out. Check backend and Wi-Fi.");
    }

    throw error;
  } finally {
    clearTimeout(timeoutId);
  }

  if (!response.ok) {
    throw new Error(`Request failed with status ${response.status}`);
  }

  return response.json();
}

export function loginSupervisor(username, password) {
  return request("/api/mobile/login", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
}

export function getDashboardOverview(filters = {}) {
  const params = new URLSearchParams();

  if (filters.year) {
    params.set("year", String(filters.year));
  }
  if (filters.month) {
    params.set("month", String(filters.month));
  }
  if (filters.day) {
    params.set("day", String(filters.day));
  }

  const query = params.toString();
  return request(`/api/dashboard/overview${query ? `?${query}` : ""}`);
}

export function getSupervisorDashboardOverview(supervisorId, filters = {}) {
  const params = new URLSearchParams();
  params.set("supervisorId", String(supervisorId));

  if (filters.year) {
    params.set("year", String(filters.year));
  }
  if (filters.month) {
    params.set("month", String(filters.month));
  }
  if (filters.day) {
    params.set("day", String(filters.day));
  }

  return request(`/api/mobile/overview?${params.toString()}`);
}

export function getStores() {
  return request("/api/map/stores");
}

export function getSupervisorStores(supervisorId) {
  const params = new URLSearchParams();
  params.set("supervisorId", String(supervisorId));

  return request(`/api/mobile/stores?${params.toString()}`);
}

export function getStoreDetails(storeCode, filters = {}) {
  const params = new URLSearchParams();

  if (filters.year) {
    params.set("year", String(filters.year));
  }
  if (filters.month) {
    params.set("month", String(filters.month));
  }

  const query = params.toString();
  return request(
    `/api/map/stores/${encodeURIComponent(storeCode)}/details${query ? `?${query}` : ""}`
  );
}

export function getSupervisorStoreDetails(supervisorId, storeCode) {
  const params = new URLSearchParams();
  params.set("supervisorId", String(supervisorId));

  return request(
    `/api/mobile/stores/${encodeURIComponent(storeCode)}?${params.toString()}`
  );
}
