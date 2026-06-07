package com.smollan.backend.service;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.smollan.backend.dto.validation.ValidationIssueDetailResponse;
import com.smollan.backend.dto.validation.ValidationIssueListResponse;
import com.smollan.backend.dto.validation.ValidationIssueReviewRequest;
import com.smollan.backend.dto.validation.ValidationIssueSummaryResponse;

@Service
public class BackofficeValidationService {

    public static final Set<String> REVIEW_STATUSES = Set.of(
            "PENDING",
            "REVIEWED",
            "CONFIRMED",
            "IGNORED",
            "NEEDS_ACTION"
    );

    private final JdbcTemplate jdbcTemplate;

    public BackofficeValidationService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public ValidationIssueListResponse listIssues(
            String ruleCode,
            String severity,
            String reviewStatus,
            String storeCode,
            String employeeCode,
            String startDate,
            String endDate,
            Integer page,
            Integer limit
    ) {
        int safePage = Math.max(page == null ? 0 : page, 0);
        int safeLimit = Math.max(1, Math.min(limit == null ? 50 : limit, 200));
        int offset = safePage * safeLimit;

        SqlFilter filter = buildIssueFilter(
                ruleCode,
                severity,
                reviewStatus,
                storeCode,
                employeeCode,
                startDate,
                endDate
        );

        Long total = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM validation_results vr WHERE 1=1 " + filter.whereClause(),
                Long.class,
                filter.params().toArray()
        );

        List<Object> params = new ArrayList<>(filter.params());
        params.add(safeLimit);
        params.add(offset);

        List<ValidationIssueSummaryResponse> issues = jdbcTemplate.query(
                """
                SELECT
                    vr.validation_id,
                    vr.run_id,
                    vr.visit_id,
                    v.visit_date,
                    vr.rule_code,
                    vr.severity,
                    vr.store_code,
                    COALESCE(sc.store_name, sv.store_name) AS store_name,
                    vr.employee_code,
                    COALESCE(ec.username, ev.username) AS employee_name,
                    vr.product_code,
                    p.product_description AS product_name,
                    vr.question,
                    vr.metric_value,
                    vr.message,
                    vr.detected_at,
                    COALESCE(vr.review_status, 'PENDING') AS review_status,
                    vr.review_comment,
                    vr.reviewed_by,
                    vr.reviewed_at
                FROM validation_results vr
                LEFT JOIN visits v ON v.visit_id = vr.visit_id
                LEFT JOIN stores sv ON sv.store_id = v.store_id
                LEFT JOIN stores sc ON sc.store_code = vr.store_code
                LEFT JOIN employees ev ON ev.employee_id = v.employee_id
                LEFT JOIN employees ec ON ec.employee_code = vr.employee_code
                LEFT JOIN products p ON p.product_code = vr.product_code
                WHERE 1=1
                """ + filter.whereClause() + """

                ORDER BY
                    CASE COALESCE(vr.review_status, 'PENDING')
                        WHEN 'NEEDS_ACTION' THEN 1
                        WHEN 'PENDING' THEN 2
                        WHEN 'CONFIRMED' THEN 3
                        WHEN 'REVIEWED' THEN 4
                        WHEN 'IGNORED' THEN 5
                        ELSE 6
                    END,
                    CASE vr.severity
                        WHEN 'HIGH' THEN 1
                        WHEN 'MEDIUM' THEN 2
                        WHEN 'LOW' THEN 3
                        ELSE 4
                    END,
                    vr.detected_at DESC
                LIMIT ? OFFSET ?
                """,
                (rs, rowNum) -> mapSummary(rs),
                params.toArray()
        );

        return new ValidationIssueListResponse(total == null ? 0L : total, safePage, safeLimit, issues);
    }

    public Optional<ValidationIssueDetailResponse> getIssue(Long validationId) {
        List<ValidationIssueDetailResponse> rows = jdbcTemplate.query(
                """
                SELECT
                    vr.validation_id,
                    vr.run_id,
                    vr.rule_code,
                    vr.entity_type,
                    vr.entity_id,
                    vr.visit_id,
                    v.visit_date,
                    vr.store_code,
                    COALESCE(sc.store_name, sv.store_name) AS store_name,
                    vr.employee_code,
                    COALESCE(ec.username, ev.username) AS employee_name,
                    vr.product_code,
                    p.product_description AS product_name,
                    vr.question,
                    vr.actual_value,
                    vr.expected_value,
                    vr.metric_value,
                    vr.message,
                    vr.severity,
                    CAST(vr.details_json AS CHAR) AS details_json,
                    vr.detected_at,
                    COALESCE(vr.review_status, 'PENDING') AS review_status,
                    vr.review_comment,
                    vr.reviewed_by,
                    vr.reviewed_at
                FROM validation_results vr
                LEFT JOIN visits v ON v.visit_id = vr.visit_id
                LEFT JOIN stores sv ON sv.store_id = v.store_id
                LEFT JOIN stores sc ON sc.store_code = vr.store_code
                LEFT JOIN employees ev ON ev.employee_id = v.employee_id
                LEFT JOIN employees ec ON ec.employee_code = vr.employee_code
                LEFT JOIN products p ON p.product_code = vr.product_code
                WHERE vr.validation_id = ?
                """,
                (rs, rowNum) -> mapDetail(rs),
                validationId
        );

        return rows.stream().findFirst();
    }

    public Optional<ValidationIssueDetailResponse> updateReview(
            Long validationId,
            ValidationIssueReviewRequest request
    ) {
        ValidationIssueReviewRequest safeRequest = request == null
                ? new ValidationIssueReviewRequest(null, null, null)
                : request;
        Optional<ValidationIssueDetailResponse> currentIssue = getIssue(validationId);
        if (currentIssue.isEmpty()) {
            return Optional.empty();
        }

        ValidationIssueDetailResponse current = currentIssue.get();
        String nextStatus = normalizeReviewStatus(safeRequest.reviewStatus(), current.reviewStatus());
        String nextComment = safeRequest.reviewComment() == null
                ? current.reviewComment()
                : trimToNull(safeRequest.reviewComment());
        String nextReviewedBy = trimToNull(safeRequest.reviewedBy());
        if (nextReviewedBy == null) {
            nextReviewedBy = current.reviewedBy() == null ? "backoffice" : current.reviewedBy();
        }

        jdbcTemplate.update(
                """
                UPDATE validation_results
                SET review_status = ?,
                    review_comment = ?,
                    reviewed_by = ?,
                    reviewed_at = CURRENT_TIMESTAMP
                WHERE validation_id = ?
                """,
                nextStatus,
                nextComment,
                nextReviewedBy,
                validationId
        );

        return getIssue(validationId);
    }

    private static SqlFilter buildIssueFilter(
            String ruleCode,
            String severity,
            String reviewStatus,
            String storeCode,
            String employeeCode,
            String startDate,
            String endDate
    ) {
        List<String> conditions = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        conditions.add("vr.run_id = (SELECT MAX(run_id) FROM validation_run_log)");
        addEqualsFilter(conditions, params, "vr.rule_code", ruleCode);
        addEqualsFilter(conditions, params, "vr.severity", normalizeOptionalUpper(severity));
        addEqualsFilter(conditions, params, "COALESCE(vr.review_status, 'PENDING')", normalizeOptionalReviewStatus(reviewStatus));
        addEqualsFilter(conditions, params, "vr.store_code", storeCode);
        addEqualsFilter(conditions, params, "vr.employee_code", employeeCode);

        LocalDate start = parseOptionalDate(startDate, "startDate");
        LocalDate end = parseOptionalDate(endDate, "endDate");
        if (start != null && end != null && start.isAfter(end)) {
            throw new IllegalArgumentException("startDate cannot be after endDate.");
        }

        if (start != null) {
            conditions.add("vr.detected_at >= ?");
            params.add(Timestamp.valueOf(start.atStartOfDay()));
        }

        if (end != null) {
            conditions.add("vr.detected_at < ?");
            params.add(Timestamp.valueOf(end.plusDays(1).atTime(LocalTime.MIDNIGHT)));
        }

        if (conditions.isEmpty()) {
            return new SqlFilter("", params);
        }

        return new SqlFilter(" AND " + String.join(" AND ", conditions), params);
    }

    private static void addEqualsFilter(
            List<String> conditions,
            List<Object> params,
            String column,
            String value
    ) {
        String normalized = trimToNull(value);
        if (normalized == null) {
            return;
        }

        conditions.add(column + " = ?");
        params.add(normalized);
    }

    private static String normalizeReviewStatus(String value, String fallback) {
        String normalized = normalizeOptionalReviewStatus(value);
        if (normalized == null) {
            return fallback == null ? "PENDING" : fallback;
        }
        return normalized;
    }

    private static String normalizeOptionalReviewStatus(String value) {
        String normalized = normalizeOptionalUpper(value);
        if (normalized == null) {
            return null;
        }
        if (!REVIEW_STATUSES.contains(normalized)) {
            throw new IllegalArgumentException("Invalid review_status: " + value);
        }
        return normalized;
    }

    private static String normalizeOptionalUpper(String value) {
        String normalized = trimToNull(value);
        return normalized == null ? null : normalized.toUpperCase();
    }

    private static LocalDate parseOptionalDate(String value, String fieldName) {
        String normalized = trimToNull(value);
        if (normalized == null) {
            return null;
        }

        try {
            return LocalDate.parse(normalized);
        } catch (Exception exc) {
            throw new IllegalArgumentException(fieldName + " must use YYYY-MM-DD format.");
        }
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static ValidationIssueSummaryResponse mapSummary(ResultSet rs) throws SQLException {
        return new ValidationIssueSummaryResponse(
                rs.getLong("validation_id"),
                rs.getLong("run_id"),
                getNullableLong(rs, "visit_id"),
                toLocalDate(rs.getDate("visit_date")),
                rs.getString("rule_code"),
                rs.getString("severity"),
                rs.getString("store_code"),
                rs.getString("store_name"),
                rs.getString("employee_code"),
                rs.getString("employee_name"),
                rs.getString("product_code"),
                rs.getString("product_name"),
                rs.getString("question"),
                rs.getBigDecimal("metric_value"),
                rs.getString("message"),
                toLocalDateTime(rs.getTimestamp("detected_at")),
                rs.getString("review_status"),
                rs.getString("review_comment"),
                rs.getString("reviewed_by"),
                toLocalDateTime(rs.getTimestamp("reviewed_at"))
        );
    }

    private static ValidationIssueDetailResponse mapDetail(ResultSet rs) throws SQLException {
        return new ValidationIssueDetailResponse(
                rs.getLong("validation_id"),
                rs.getLong("run_id"),
                rs.getString("rule_code"),
                rs.getString("entity_type"),
                rs.getString("entity_id"),
                getNullableLong(rs, "visit_id"),
                toLocalDate(rs.getDate("visit_date")),
                rs.getString("store_code"),
                rs.getString("store_name"),
                rs.getString("employee_code"),
                rs.getString("employee_name"),
                rs.getString("product_code"),
                rs.getString("product_name"),
                rs.getString("question"),
                rs.getString("actual_value"),
                rs.getString("expected_value"),
                rs.getBigDecimal("metric_value"),
                rs.getString("message"),
                rs.getString("severity"),
                rs.getString("details_json"),
                toLocalDateTime(rs.getTimestamp("detected_at")),
                rs.getString("review_status"),
                rs.getString("review_comment"),
                rs.getString("reviewed_by"),
                toLocalDateTime(rs.getTimestamp("reviewed_at"))
        );
    }

    private static LocalDate toLocalDate(Date date) {
        return date == null ? null : date.toLocalDate();
    }

    private static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    private static Long getNullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private record SqlFilter(String whereClause, List<Object> params) {
    }
}
