const API_BASE =
  process.env.REACT_APP_API_BASE_URL ||
  process.env.REACT_APP_API_BASE ||
  "http://localhost:9000";

async function requestJson(path, options, errorMessage) {
  const response = await fetch(`${API_BASE}${path}`, options);

  if (!response.ok) {
    throw new Error(errorMessage);
  }

  return response.json();
}

function appendParam(params, key, value) {
  if (value !== undefined && value !== null && String(value).trim()) {
    params.set(key, String(value).trim());
  }
}

export function fetchValidationReviewIssues(filters = {}) {
  const params = new URLSearchParams();
  appendParam(params, "ruleCode", filters.ruleCode);
  appendParam(params, "severity", filters.severity);
  appendParam(params, "reviewStatus", filters.reviewStatus);
  appendParam(params, "storeCode", filters.storeCode);
  appendParam(params, "employeeCode", filters.employeeCode);
  appendParam(params, "startDate", filters.startDate);
  appendParam(params, "endDate", filters.endDate);
  params.set("page", String(filters.page ?? 0));
  params.set("limit", String(filters.limit ?? 50));

  return requestJson(
    `/api/backoffice/validation/issues?${params.toString()}`,
    undefined,
    "Unable to load validation review issues."
  );
}

export function fetchValidationReviewIssueDetail(validationId) {
  return requestJson(
    `/api/backoffice/validation/issues/${encodeURIComponent(validationId)}`,
    undefined,
    "Unable to load validation issue detail."
  );
}

export function updateValidationIssueReview(validationId, payload) {
  return requestJson(
    `/api/backoffice/validation/issues/${encodeURIComponent(validationId)}/review`,
    {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    },
    "Unable to update validation review."
  );
}
