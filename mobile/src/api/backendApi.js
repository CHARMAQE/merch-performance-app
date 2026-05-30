const LAN_API_BASE =
  process.env.EXPO_PUBLIC_API_BASE_URL || "http://192.168.1.150:9000";
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
  const params = buildSupervisorFilterParams(supervisorId, filters);

  return request(`/api/mobile/overview?${params.toString()}`);
}

export function getOverview(filters = {}) {
  const params = buildSupervisorFilterParams(filters.supervisorId, filters);
  return request(`/api/mobile/overview?${params.toString()}`);
}

export function getSupervisorMerchandiserExecution(supervisorId, filters = {}) {
  const params = buildSupervisorFilterParams(supervisorId, filters);

  return request(`/api/mobile/merchandisers?${params.toString()}`);
}

export function getMerchandisers(filters = {}) {
  const params = buildSupervisorFilterParams(filters.supervisorId, filters);
  return request(`/api/mobile/merchandisers?${params.toString()}`);
}

export function getSupervisorMerchandiserStores(supervisorId, employeeCode, filters = {}) {
  const params = buildSupervisorFilterParams(supervisorId, filters);

  return request(
    `/api/mobile/merchandisers/${encodeURIComponent(employeeCode)}/stores?${params.toString()}`
  );
}

export function getSupervisorExecutionStores(supervisorId, type, filters = {}) {
  const params = buildSupervisorFilterParams(supervisorId, filters);
  params.set("type", type);

  return request(`/api/mobile/execution-stores?${params.toString()}`);
}

export function getMerchandiserStores(employeeCode, filters = {}) {
  const params = buildSupervisorFilterParams(filters.supervisorId, filters);
  return request(
    `/api/mobile/merchandisers/${encodeURIComponent(employeeCode)}/stores?${params.toString()}`
  );
}

export function getSupervisorIssues(supervisorId, filters = {}) {
  const params = buildSupervisorFilterParams(supervisorId, filters);
  return request(`/api/mobile/issues?${params.toString()}`);
}

export function getIssues(filters = {}) {
  const params = buildSupervisorFilterParams(filters.supervisorId, filters);
  return request(`/api/mobile/issues?${params.toString()}`);
}

export function getStores(filters = {}) {
  const params = buildSupervisorFilterParams(filters.supervisorId, filters);
  return request(`/api/mobile/stores?${params.toString()}`);
}

export function getSupervisorStores(supervisorId, filters = {}) {
  const params = buildSupervisorFilterParams(supervisorId, filters);

  return request(`/api/mobile/stores?${params.toString()}`);
}

export function getMobileStores(filters = {}) {
  const params = buildSupervisorFilterParams(filters.supervisorId, filters);
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
  return getStoreDetail(storeCode, { supervisorId });
}

export function getStoreDetail(storeCode, filters = {}) {
  const params = buildSupervisorFilterParams(filters.supervisorId, filters);

  return request(
    `/api/mobile/stores/${encodeURIComponent(storeCode)}?${params.toString()}`
  );
}

function buildSupervisorFilterParams(supervisorId, filters = {}) {
  const params = new URLSearchParams();

  if (supervisorId !== null && supervisorId !== undefined) {
    params.set("supervisorId", String(supervisorId));
  }
  if (filters.startDate) {
    params.set("startDate", String(filters.startDate));
  }
  if (filters.endDate) {
    params.set("endDate", String(filters.endDate));
  }
  if (filters.year) {
    params.set("year", String(filters.year));
  }
  if (filters.month) {
    params.set("month", String(filters.month));
  }
  if (filters.day) {
    params.set("day", String(filters.day));
  }
  if (filters.region) {
    params.set("region", String(filters.region));
  }
  if (filters.storeFormat) {
    params.set("storeFormat", String(filters.storeFormat));
  }
  if (filters.storeFormatGroup) {
    params.set("storeFormatGroup", String(filters.storeFormatGroup));
  }

  return params;
}
