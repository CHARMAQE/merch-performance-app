package com.smollan.backend.service;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.smollan.backend.dto.dashboard.DashboardOverviewResponse;
import com.smollan.backend.dto.validation.ValidationIssueResponse;

@Service
public class DashboardService {

    private static final String DATE_COVERAGE_SQL = """
            SELECT
                MIN(visit_date) AS min_visit_date,
                MAX(visit_date) AS max_visit_date,
                COUNT(DISTINCT visit_date) AS distinct_visit_dates
            FROM visits v
            """;

    private static final String TABLE_COUNTS_SQL = """
            SELECT
                (SELECT COUNT(DISTINCT v.employee_id) FROM visits v %s) AS employees_count,
                (SELECT COUNT(DISTINCT v.store_id) FROM visits v %s) AS stores_count,
                (
                    SELECT COUNT(DISTINCT sr.product_code)
                    FROM survey_responses sr
                    JOIN visits v ON v.visit_id = sr.visit_id
                    %s
                      AND sr.product_code IS NOT NULL
                      AND sr.product_code <> ''
                ) AS products_count,
                (SELECT COUNT(*) FROM visits v %s) AS visits_count,
                (
                    SELECT COUNT(*)
                    FROM survey_responses sr
                    JOIN visits v ON v.visit_id = sr.visit_id
                    %s
                ) AS survey_responses_count
            """;

    private static final String LATEST_VALIDATION_RUN_SQL = """
            SELECT
                run_id,
                started_at,
                finished_at,
                status,
                rules_executed,
                issues_found
            FROM validation_run_log
            ORDER BY run_id DESC
            LIMIT 1
            """;

    private final JdbcTemplate jdbcTemplate;

    public DashboardService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public DashboardOverviewResponse getOverview(Integer year, Integer month, Integer day, String storeCode) {
        DateFilter dateFilter = buildDateFilter(year, month, day);
        ValidationFilter validationFilter = buildValidationFilter(storeCode);

        DashboardOverviewResponse.DateCoverage dateCoverage = jdbcTemplate.query(
                DATE_COVERAGE_SQL + dateFilter.whereClause(),
                rs -> {
                    if (!rs.next()) {
                        return new DashboardOverviewResponse.DateCoverage(null, null, 0L);
                    }

                    return new DashboardOverviewResponse.DateCoverage(
                            toLocalDate(rs.getDate("min_visit_date")),
                            toLocalDate(rs.getDate("max_visit_date")),
                            rs.getLong("distinct_visit_dates")
                    );
                },
                dateFilter.params().toArray()
        );

        String tableCountsSql = TABLE_COUNTS_SQL.formatted(
                dateFilter.whereClause(),
                dateFilter.whereClause(),
                dateFilter.whereClause(),
                dateFilter.whereClause(),
                dateFilter.whereClause()
        );
        List<Object> tableCountParams = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            tableCountParams.addAll(dateFilter.params());
        }

        DashboardOverviewResponse.TableCounts tableCounts = jdbcTemplate.query(
                tableCountsSql,
                rs -> {
                    if (!rs.next()) {
                        return new DashboardOverviewResponse.TableCounts(0L, 0L, 0L, 0L, 0L);
                    }

                    return new DashboardOverviewResponse.TableCounts(
                            rs.getLong("employees_count"),
                            rs.getLong("stores_count"),
                            rs.getLong("products_count"),
                            rs.getLong("visits_count"),
                            rs.getLong("survey_responses_count")
                    );
                },
                tableCountParams.toArray()
        );

        DashboardOverviewResponse.LatestValidationRun latestValidationRun = jdbcTemplate.query(
                LATEST_VALIDATION_RUN_SQL,
                rs -> {
                    if (!rs.next()) {
                        return null;
                    }

                    return new DashboardOverviewResponse.LatestValidationRun(
                            rs.getLong("run_id"),
                            toLocalDateTime(rs.getTimestamp("started_at")),
                            toLocalDateTime(rs.getTimestamp("finished_at")),
                            rs.getString("status"),
                            getNullableInteger(rs, "rules_executed"),
                            getNullableInteger(rs, "issues_found")
                    );
                }
        );

        List<DashboardOverviewResponse.IssueCount> issueCountsByRule = List.of();
        List<DashboardOverviewResponse.IssueCount> issueCountsBySeverity = List.of();

        if (latestValidationRun != null) {
            issueCountsByRule = jdbcTemplate.query(
                    """
                    SELECT
                        vr.rule_code AS label,
                        COUNT(*) AS issue_count
                    FROM validation_results vr
                    WHERE vr.run_id = (SELECT MAX(run_id) FROM validation_run_log)
                    """ + validationFilter.andClause() + """

                    GROUP BY vr.rule_code
                    ORDER BY issue_count DESC, vr.rule_code
                    """,
                    (rs, rowNum) -> new DashboardOverviewResponse.IssueCount(
                            rs.getString("label"),
                            rs.getLong("issue_count")
                    ),
                    validationFilter.params().toArray()
            );

            issueCountsBySeverity = jdbcTemplate.query(
                    """
                    SELECT
                        vr.severity AS label,
                        COUNT(*) AS issue_count
                    FROM validation_results vr
                    WHERE vr.run_id = (SELECT MAX(run_id) FROM validation_run_log)
                    """ + validationFilter.andClause() + """

                    GROUP BY vr.severity
                    ORDER BY issue_count DESC, vr.severity
                    """,
                    (rs, rowNum) -> new DashboardOverviewResponse.IssueCount(
                            rs.getString("label"),
                            rs.getLong("issue_count")
                    ),
                    validationFilter.params().toArray()
            );
        }

        return new DashboardOverviewResponse(
                dateCoverage,
                tableCounts,
                latestValidationRun,
                issueCountsByRule,
                issueCountsBySeverity
        );
    }

    public List<ValidationIssueResponse> getLatestIssues(int limit, String storeCode) {
        int safeLimit = Math.max(1, Math.min(limit, 500));
        ValidationFilter validationFilter = buildValidationFilter(storeCode);

        String latestIssuesSql = """
                SELECT
                    vr.run_id,
                    vr.visit_id,
                    v.visit_date,
                    vr.rule_code,
                    vr.severity,
                    vr.store_code,
                    vr.employee_code,
                    vr.product_code,
                    vr.question,
                    vr.metric_value,
                    vr.message,
                    vr.detected_at
                FROM validation_results vr
                LEFT JOIN visits v ON v.visit_id = vr.visit_id
                WHERE vr.run_id = (SELECT MAX(run_id) FROM validation_run_log)
                """ + validationFilter.andClause() + """

                ORDER BY
                    CASE vr.severity
                        WHEN 'HIGH' THEN 1
                        WHEN 'MEDIUM' THEN 2
                        WHEN 'LOW' THEN 3
                        ELSE 4
                    END,
                    vr.metric_value DESC,
                    vr.detected_at DESC
                LIMIT ?
                """;

        List<Object> params = new ArrayList<>(validationFilter.params());
        params.add(safeLimit);

        return jdbcTemplate.query(
                latestIssuesSql,
                (rs, rowNum) -> new ValidationIssueResponse(
                        rs.getLong("run_id"),
                        getNullableLong(rs, "visit_id"),
                        toLocalDate(rs.getDate("visit_date")),
                        rs.getString("rule_code"),
                        rs.getString("severity"),
                        rs.getString("store_code"),
                        rs.getString("employee_code"),
                        rs.getString("product_code"),
                        rs.getString("question"),
                        rs.getBigDecimal("metric_value"),
                        rs.getString("message"),
                        toLocalDateTime(rs.getTimestamp("detected_at"))
                ),
                params.toArray()
        );
    }

    private static LocalDate toLocalDate(Date date) {
        return date == null ? null : date.toLocalDate();
    }

    private static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    private static Integer getNullableInteger(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static Long getNullableLong(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private static DateFilter buildDateFilter(Integer year, Integer month, Integer day) {
        List<String> conditions = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        if (year != null) {
            conditions.add("YEAR(v.visit_date) = ?");
            params.add(year);
        }

        if (month != null) {
            conditions.add("MONTH(v.visit_date) = ?");
            params.add(month);
        }

        if (day != null) {
            conditions.add("DAYOFMONTH(v.visit_date) = ?");
            params.add(day);
        }

        if (conditions.isEmpty()) {
            return new DateFilter(" WHERE 1=1", List.of());
        }

        return new DateFilter(" WHERE 1=1 AND " + String.join(" AND ", conditions), params);
    }

    private record DateFilter(String whereClause, List<Object> params) {
    }

    private static ValidationFilter buildValidationFilter(String storeCode) {
        if (storeCode == null || storeCode.isBlank()) {
            return new ValidationFilter("", List.of());
        }

        return new ValidationFilter(" AND vr.store_code = ?", List.of(storeCode.trim()));
    }

    private record ValidationFilter(String andClause, List<Object> params) {
    }
}
