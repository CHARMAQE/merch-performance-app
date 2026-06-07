import { useEffect, useMemo, useState } from "react";
import {
  fetchValidationReviewIssueDetail,
  fetchValidationReviewIssues,
  updateValidationIssueReview,
} from "../api/validationReviewApi";

const OSA_RULE = "OSA_UNUSUAL_NON_BY_BANNER";
const GPS_RULE = "GPS_INCONSISTENT_CHECKIN_SAME_STORE_MONTH";

const REVIEW_STATUSES = [
  "PENDING",
  "REVIEWED",
  "CONFIRMED",
  "IGNORED",
  "NEEDS_ACTION",
];

const RULE_OPTIONS = [OSA_RULE, GPS_RULE];

const emptyFilters = {
  ruleCode: "",
  reviewStatus: "",
  storeCode: "",
  employeeCode: "",
  startDate: "",
  endDate: "",
};

const emptyKpis = {
  pending: 0,
  reviewed: 0,
  confirmed: 0,
  ignored: 0,
  needsAction: 0,
};

function formatNumber(value) {
  const numberValue = Number(value);

  if (!Number.isFinite(numberValue)) {
    return "0";
  }

  return new Intl.NumberFormat("en-US").format(numberValue);
}

function formatDateTime(value) {
  if (!value) {
    return "N/A";
  }

  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function formatDate(value) {
  if (!value) {
    return "N/A";
  }

  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
  }).format(new Date(value));
}

function formatPercent(value) {
  const numberValue = Number(value);

  if (!Number.isFinite(numberValue)) {
    return "N/A";
  }

  return `${new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 2,
  }).format(numberValue)}%`;
}

function ruleLabel(ruleCode) {
  if (ruleCode === GPS_RULE) {
    return "GPS consistency check";
  }
  if (ruleCode === OSA_RULE) {
    return "OSA availability anomaly";
  }
  return ruleCode || "Unknown issue";
}

function ruleForChannel(channel) {
  if (channel === "MT") {
    return OSA_RULE;
  }
  if (channel === "GT") {
    return GPS_RULE;
  }
  return "";
}

function statusLabel(status) {
  return String(status || "PENDING")
    .replace(/_/g, " ")
    .toLowerCase()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function displayValue(value) {
  return value === null || value === undefined || value === "" ? "N/A" : value;
}

function buildKpiScope(filters) {
  return {
    ruleCode: filters.ruleCode,
    storeCode: filters.storeCode,
    employeeCode: filters.employeeCode,
    startDate: filters.startDate,
    endDate: filters.endDate,
  };
}

function responseTotal(response) {
  return Number(response?.total || 0);
}

function parseDetails(detailsJson) {
  if (!detailsJson) {
    return {};
  }

  if (typeof detailsJson === "object") {
    return detailsJson;
  }

  try {
    return JSON.parse(detailsJson);
  } catch {
    return {};
  }
}

function isOsaIssue(issue) {
  return issue?.ruleCode === OSA_RULE;
}

function issueSubtitle(issue) {
  if (isOsaIssue(issue)) {
    return "Availability was marked as unavailable despite high weekly availability in the same banner.";
  }

  if (issue?.ruleCode === GPS_RULE) {
    return "GPS consistency supports visit validation for this store.";
  }

  return "Review the detection and record the supervisor decision.";
}

function businessMessage(issue) {
  if (isOsaIssue(issue)) {
    return "Availability marked as unavailable, while the same SKU shows high weekly availability for this banner.";
  }

  if (issue?.ruleCode === GPS_RULE) {
    return "Store visit location needs consistency review.";
  }

  return "Detailed detection selected for supervisor review.";
}

function issueProductName(issue) {
  return issue?.productName || issue?.productCode || "Product unavailable";
}

function KpiCard({ label, value, tone }) {
  return (
    <div className={`validation-kpi ${tone ? `is-${tone}` : ""}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function Badge({ children, type, value }) {
  const normalized = String(value || children || "").toLowerCase().replace(/_/g, "-");
  return (
    <span className={`validation-badge ${type || ""} is-${normalized}`}>
      {children}
    </span>
  );
}

function EntityText({ name, code }) {
  return (
    <span className="validation-entity-text">
      <strong>{displayValue(name || code)}</strong>
      {code ? <small>{code}</small> : null}
    </span>
  );
}

function DetailField({ label, children }) {
  return (
    <div className="validation-detail-field">
      <span>{label}</span>
      {children}
    </div>
  );
}

function ValidationCenterPage({ username, selectedChannel = "ALL" }) {
  const [draftFilters, setDraftFilters] = useState(emptyFilters);
  const [appliedFilters, setAppliedFilters] = useState(emptyFilters);
  const [issues, setIssues] = useState([]);
  const [total, setTotal] = useState(0);
  const [kpis, setKpis] = useState(emptyKpis);
  const [isLoading, setIsLoading] = useState(false);
  const [isLoadingKpis, setIsLoadingKpis] = useState(false);
  const [error, setError] = useState("");
  const [selectedIssue, setSelectedIssue] = useState(null);
  const [issueDetail, setIssueDetail] = useState(null);
  const [isDetailLoading, setIsDetailLoading] = useState(false);
  const [reviewStatus, setReviewStatus] = useState("PENDING");
  const [reviewComment, setReviewComment] = useState("");
  const [isSaving, setIsSaving] = useState(false);

  const currentIssue = issueDetail || selectedIssue;
  const detailValues = useMemo(
    () => parseDetails(currentIssue?.detailsJson),
    [currentIssue?.detailsJson]
  );
  const effectiveFilters = useMemo(
    () => ({
      ...appliedFilters,
      ruleCode: ruleForChannel(selectedChannel) || appliedFilters.ruleCode,
    }),
    [appliedFilters, selectedChannel]
  );
  const weeklyAvailability =
    detailValues.availability_rate ??
    detailValues.weekly_availability ??
    detailValues.weeklyAvailability ??
    currentIssue?.metricValue;

  async function loadIssueDetail(validationId) {
    if (!validationId) {
      setSelectedIssue(null);
      setIssueDetail(null);
      return;
    }

    try {
      setIsDetailLoading(true);
      setError("");
      const data = await fetchValidationReviewIssueDetail(validationId);
      setSelectedIssue(data);
      setIssueDetail(data);
      setReviewStatus(data?.reviewStatus || "PENDING");
      setReviewComment(data?.reviewComment || "");
    } catch (loadError) {
      setError(loadError.message || "Unable to load detection detail.");
    } finally {
      setIsDetailLoading(false);
    }
  }

  async function loadIssues() {
    try {
      setIsLoading(true);
      setError("");
      const data = await fetchValidationReviewIssues({
        ...effectiveFilters,
        page: 0,
        limit: 60,
      });
      const nextIssues = Array.isArray(data.issues) ? data.issues : [];
      const nextTotal = Number(data.total || 0);

      setIssues(nextIssues);
      setTotal(nextTotal);

      if (!nextIssues.length) {
        setSelectedIssue(null);
        setIssueDetail(null);
        return;
      }

      const selectedStillVisible = nextIssues.some(
        (issue) => issue.validationId === currentIssue?.validationId
      );
      const nextSelected = selectedStillVisible ? currentIssue : nextIssues[0];

      setSelectedIssue(nextSelected);
      await loadIssueDetail(nextSelected.validationId);
    } catch (loadError) {
      setError(loadError.message || "Unable to load detections.");
      setIssues([]);
      setTotal(0);
      setSelectedIssue(null);
      setIssueDetail(null);
    } finally {
      setIsLoading(false);
    }
  }

  async function loadKpis() {
    try {
      setIsLoadingKpis(true);
      const scope = buildKpiScope(effectiveFilters);
      const [
        pendingResponse,
        reviewedResponse,
        confirmedResponse,
        ignoredResponse,
        needsActionResponse,
      ] = await Promise.all([
        fetchValidationReviewIssues({
          ...scope,
          reviewStatus: "PENDING",
          page: 0,
          limit: 1,
        }),
        fetchValidationReviewIssues({
          ...scope,
          reviewStatus: "REVIEWED",
          page: 0,
          limit: 1,
        }),
        fetchValidationReviewIssues({
          ...scope,
          reviewStatus: "CONFIRMED",
          page: 0,
          limit: 1,
        }),
        fetchValidationReviewIssues({
          ...scope,
          reviewStatus: "IGNORED",
          page: 0,
          limit: 1,
        }),
        fetchValidationReviewIssues({
          ...scope,
          reviewStatus: "NEEDS_ACTION",
          page: 0,
          limit: 1,
        }),
      ]);

      setKpis({
        pending: responseTotal(pendingResponse),
        reviewed: responseTotal(reviewedResponse),
        confirmed: responseTotal(confirmedResponse),
        ignored: responseTotal(ignoredResponse),
        needsAction: responseTotal(needsActionResponse),
      });
    } catch (loadError) {
      setError(loadError.message || "Unable to load review counters.");
      setKpis(emptyKpis);
    } finally {
      setIsLoadingKpis(false);
    }
  }

  useEffect(() => {
    loadIssues();
    loadKpis();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [appliedFilters, selectedChannel]);

  function updateDraftFilter(key, value) {
    setDraftFilters((current) => ({
      ...current,
      [key]: value,
    }));
  }

  function applyFilters(event) {
    event.preventDefault();
    setAppliedFilters(draftFilters);
  }

  function resetFilters() {
    setDraftFilters(emptyFilters);
    setAppliedFilters(emptyFilters);
  }

  async function saveReview() {
    if (!currentIssue?.validationId) {
      return;
    }

    try {
      setIsSaving(true);
      setError("");
      const updatedIssue = await updateValidationIssueReview(currentIssue.validationId, {
        reviewStatus,
        reviewComment,
        reviewedBy: username || "backoffice",
      });
      setSelectedIssue(updatedIssue);
      setIssueDetail(updatedIssue);
      await loadIssues();
      await loadKpis();
    } catch (saveError) {
      setError(saveError.message || "Unable to save review.");
    } finally {
      setIsSaving(false);
    }
  }

  return (
    <main className="validation-center-page">
      <section className="validation-kpi-grid is-review-grid" aria-label="Review counters">
        <KpiCard
          label="Pending"
          value={isLoadingKpis ? "..." : formatNumber(kpis.pending)}
          tone="pending"
        />
        <KpiCard
          label="Reviewed"
          value={isLoadingKpis ? "..." : formatNumber(kpis.reviewed)}
          tone="reviewed"
        />
        <KpiCard
          label="Confirmed"
          value={isLoadingKpis ? "..." : formatNumber(kpis.confirmed)}
          tone="confirmed"
        />
        <KpiCard
          label="Ignored"
          value={isLoadingKpis ? "..." : formatNumber(kpis.ignored)}
          tone="ignored"
        />
        <KpiCard
          label="Needs action"
          value={isLoadingKpis ? "..." : formatNumber(kpis.needsAction)}
          tone="needs-action"
        />
      </section>

      <form className="validation-filter-card" onSubmit={applyFilters}>
        {/* <label className="validation-filter-field">
          <span>Rule</span>
          <select
            value={draftFilters.ruleCode}
            onChange={(event) => updateDraftFilter("ruleCode", event.target.value)}
          >
            <option value="">All rules</option>
            {RULE_OPTIONS.map((rule) => (
              <option value={rule} key={rule}>
                {ruleLabel(rule)}
              </option>
            ))}
          </select>
        </label> */}
        <label className="validation-filter-field">
          <span>Status</span>
          <select
            value={draftFilters.reviewStatus}
            onChange={(event) => updateDraftFilter("reviewStatus", event.target.value)}
          >
            <option value="">All statuses</option>
            {REVIEW_STATUSES.map((status) => (
              <option value={status} key={status}>
                {statusLabel(status)}
              </option>
            ))}
          </select>
        </label>
        <label className="validation-filter-field">
          <span>Store code</span>
          <input
            value={draftFilters.storeCode}
            onChange={(event) => updateDraftFilter("storeCode", event.target.value)}
            placeholder="Store code"
          />
        </label>
        <label className="validation-filter-field">
          <span>Employee code</span>
          <input
            value={draftFilters.employeeCode}
            onChange={(event) => updateDraftFilter("employeeCode", event.target.value)}
            placeholder="Employee code"
          />
        </label>
        <label className="validation-filter-field validation-date-range-field">
          <span>Date range</span>
          <div className="validation-date-range">
            <input
              aria-label="Start date"
              type="date"
              value={draftFilters.startDate}
              onChange={(event) => updateDraftFilter("startDate", event.target.value)}
            />
            <input
              aria-label="End date"
              type="date"
              value={draftFilters.endDate}
              onChange={(event) => updateDraftFilter("endDate", event.target.value)}
            />
          </div>
        </label>
        <div className="validation-filter-actions">
          <button type="submit" className="map-search-button">
            Apply
          </button>
          <button type="button" className="map-secondary-button" onClick={resetFilters}>
            Reset
          </button>
        </div>
      </form>

      {error ? <div className="validation-error">{error}</div> : null}

      <section className="validation-workspace">
        <div className="validation-issue-table-card">
          <div className="validation-table-toolbar">
            <div>
              <h2>Issue Queue</h2>
              <span>Detailed detections: {formatNumber(total)}</span>
              <p className="issue-table-note">
                Detailed detections are kept for traceability. Grouped situations are shown in the overview.
              </p>
            </div>
          </div>

          <div className="validation-issue-table">
            <div className="validation-issue-head">
              <span>Store</span>
              <span>Merchandiser</span>
              <span>Product</span>
              <span>Issue type</span>
              <span>Status</span>
              <span>Detected</span>
            </div>
            <div className="validation-issue-body">
              {isLoading ? <p className="validation-empty">Loading detections...</p> : null}
              {!isLoading && issues.length === 0 ? (
                <p className="validation-empty">No detailed detections match the filters.</p>
              ) : null}
              {issues.map((issue) => (
                <button
                  type="button"
                  className={`validation-issue-line ${
                    currentIssue?.validationId === issue.validationId ? "is-selected" : ""
                  }`}
                  key={issue.validationId}
                  onClick={() => loadIssueDetail(issue.validationId)}
                >
                  <EntityText name={issue.storeName} code={issue.storeCode} />
                  <EntityText name={issue.employeeName} code={issue.employeeCode} />
                  <EntityText name={issueProductName(issue)} code={issue.productCode} />
                  <span>{ruleLabel(issue.ruleCode)}</span>
                  <span>
                    <Badge type="status" value={issue.reviewStatus}>
                      {statusLabel(issue.reviewStatus)}
                    </Badge>
                  </span>
                  <span>{formatDate(issue.detectedAt)}</span>
                </button>
              ))}
            </div>
          </div>
        </div>

        <aside className="validation-detail-card">
          <div className="validation-detail-title">
            <div>
              <h2>{currentIssue ? ruleLabel(currentIssue.ruleCode) : "No detection selected"}</h2>
              {currentIssue ? <p>{issueSubtitle(currentIssue)}</p> : null}
            </div>
            {currentIssue ? (
              <Badge type="status" value={currentIssue.reviewStatus}>
                {statusLabel(currentIssue.reviewStatus)}
              </Badge>
            ) : null}
          </div>

          {isDetailLoading ? <p className="validation-empty">Loading detection detail...</p> : null}

          {!currentIssue && !isDetailLoading ? (
            <p className="validation-empty">Select a detection from the queue.</p>
          ) : null}

          {currentIssue ? (
            <>
              <p className="validation-detail-message">
                {businessMessage(currentIssue)}
              </p>

              <div className="validation-detail-grid">
                <DetailField label="Store">
                  <EntityText name={currentIssue.storeName} code={currentIssue.storeCode} />
                </DetailField>
                <DetailField label="Merchandiser">
                  <EntityText name={currentIssue.employeeName} code={currentIssue.employeeCode} />
                </DetailField>
                {currentIssue.productCode || currentIssue.productName ? (
                  <DetailField label="Product">
                    <EntityText name={issueProductName(currentIssue)} code={currentIssue.productCode} />
                  </DetailField>
                ) : null}
                {isOsaIssue(currentIssue) ? (
                  <>
                    <DetailField label="Banner">
                      <strong>{displayValue(detailValues.banner)}</strong>
                    </DetailField>
                    <DetailField label="Weekly availability">
                      <strong>{formatPercent(weeklyAvailability)}</strong>
                    </DetailField>
                  </>
                ) : null}
                <DetailField label="Status">
                  <Badge type="status" value={currentIssue.reviewStatus}>
                    {statusLabel(currentIssue.reviewStatus)}
                  </Badge>
                </DetailField>
              </div>

              <div className="validation-review-form">
                <label>
                  <span>Review status</span>
                  <select
                    value={reviewStatus}
                    onChange={(event) => setReviewStatus(event.target.value)}
                  >
                    {REVIEW_STATUSES.map((status) => (
                      <option value={status} key={status}>
                        {statusLabel(status)}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  <span>Review comment</span>
                  <textarea
                    value={reviewComment}
                    onChange={(event) => setReviewComment(event.target.value)}
                    rows={5}
                    placeholder="Write the decision, reason, or required follow-up."
                  />
                </label>
                <button
                  type="button"
                  className="map-search-button"
                  onClick={saveReview}
                  disabled={isSaving}
                >
                  {isSaving ? "Saving" : "Save review"}
                </button>
              </div>

              {currentIssue.reviewedBy || currentIssue.reviewedAt ? (
                <div className="validation-review-audit">
                  <span>Last reviewed</span>
                  <strong>
                    {displayValue(currentIssue.reviewedBy)} / {formatDateTime(currentIssue.reviewedAt)}
                  </strong>
                </div>
              ) : null}
            </>
          ) : null}
        </aside>
      </section>
    </main>
  );
}

export default ValidationCenterPage;
