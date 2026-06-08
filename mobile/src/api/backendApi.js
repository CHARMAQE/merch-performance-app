// Expo Go on a physical phone must call the Windows PC LAN IP.
// localhost works only for web/emulator contexts where the app runs on the same machine.
const DEFAULT_MOBILE_API_BASE_URL = "http://192.168.1.171:9000";
const LAN_API_BASE =
  process.env.EXPO_PUBLIC_API_BASE_URL || DEFAULT_MOBILE_API_BASE_URL;
const REQUEST_TIMEOUT_MS = 30000;

export const API_BASE = LAN_API_BASE;

class ApiError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "ApiError";
    this.status = details.status;
    this.body = details.body;
    this.code = details.code;
  }
}

async function request(path, options = {}) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  const url = `${API_BASE}${path}`;

  let response;
  try {
    response = await fetch(url, {
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
    const code = error.name === "AbortError" ? "TIMEOUT" : "NETWORK";

    if (error.name === "AbortError") {
      throw new ApiError("Request timed out. Check backend and Wi-Fi.", {
        code,
      });
    }

    throw new ApiError("Backend unreachable. Check backend and Wi-Fi.", {
      code,
    });
  } finally {
    clearTimeout(timeoutId);
  }

  const responseText = await response.text();

  if (!response.ok) {
    throw new ApiError(`Request failed with status ${response.status}`, {
      status: response.status,
      body: responseText,
    });
  }

  if (!responseText) {
    return null;
  }

  return JSON.parse(responseText);
}

export function loginSupervisor(email, password) {
  const payload = { email, password };

  return request("/api/mobile/login", {
    method: "POST",
    body: JSON.stringify(payload),
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
