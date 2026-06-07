package com.smollan.backend.service;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.smollan.backend.dto.map.StoreMapProblematicStoresResponse;
import com.smollan.backend.dto.map.StoreMapProblematicStoresResponse.ChannelSummary;
import com.smollan.backend.dto.map.StoreMapProblematicStoresResponse.ProblematicStoreRow;

@Service
public class BackofficeStoreMapService {

    private static final String OSA_RULE = "OSA_UNUSUAL_NON_BY_BANNER";
    private static final String GPS_RULE = "GPS_INCONSISTENT_CHECKIN_SAME_STORE_MONTH";
    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 5000;

    private static final String BASE_SQL = """
            SELECT
                vr.run_id,
                vr.rule_code,
                COALESCE(NULLIF(vr.employee_code, ''), ev.employee_code, 'Unknown') AS employee_code,
                COALESCE(ec.username, ev.username, NULLIF(vr.employee_code, ''), ev.employee_code, 'Unknown') AS merchandiser_name,
                COALESCE(NULLIF(vr.store_code, ''), sv.store_code, 'Unknown') AS store_code,
                COALESCE(sc.store_name, sv.store_name, NULLIF(vr.store_code, ''), sv.store_code, 'Unknown') AS store_name,
                COALESCE(sc.store_format, sv.store_format, 'Unknown') AS store_format,
                CASE
                    WHEN UPPER(TRIM(COALESCE(sc.store_format, sv.store_format, ''))) IN
                        ('HYPERMARKET', 'SUPERMARKET LOWER', 'SUPERMARKET UPPER') THEN 'MT'
                    WHEN UPPER(TRIM(COALESCE(sc.store_format, sv.store_format, ''))) IN
                        ('GROCERY', 'CASH AND CARRY') THEN 'GT'
                    ELSE 'UNKNOWN'
                END AS channel,
                COALESCE(v.visit_date, DATE(vr.detected_at)) AS visit_date,
                COALESCE(NULLIF(v.latitude, 0), coords.latitude) AS latitude,
                COALESCE(NULLIF(v.longitude, 0), coords.longitude) AS longitude
            FROM validation_results vr
            LEFT JOIN visits v ON v.visit_id = vr.visit_id
            LEFT JOIN stores sv ON sv.store_id = v.store_id
            LEFT JOIN stores sc ON sc.store_code = vr.store_code
            LEFT JOIN employees ev ON ev.employee_id = v.employee_id
            LEFT JOIN employees ec ON ec.employee_code = vr.employee_code
            LEFT JOIN (
                SELECT
                    s.store_code,
                    AVG(NULLIF(v.latitude, 0)) AS latitude,
                    AVG(NULLIF(v.longitude, 0)) AS longitude
                FROM visits v
                JOIN stores s ON s.store_id = v.store_id
                WHERE v.latitude IS NOT NULL
                  AND v.longitude IS NOT NULL
                  AND v.latitude <> 0
                  AND v.longitude <> 0
                GROUP BY s.store_code
            ) coords ON coords.store_code = COALESCE(NULLIF(vr.store_code, ''), sv.store_code)
            """;

    private final JdbcTemplate jdbcTemplate;

    public BackofficeStoreMapService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public StoreMapProblematicStoresResponse getProblematicStores(
            String channel,
            String startDate,
            String endDate,
            String storeCode,
            String employeeCode,
            Integer limit
    ) {
        String normalizedChannel = normalizeChannel(channel);
        LocalDate start = parseOptionalDate(startDate, "startDate");
        LocalDate end = parseOptionalDate(endDate, "endDate");
        if (start != null && end != null && start.isAfter(end)) {
            throw new IllegalArgumentException("startDate cannot be after endDate.");
        }

        int safeLimit = Math.max(1, Math.min(limit == null ? DEFAULT_LIMIT : limit, MAX_LIMIT));
        StoreMapFilter summaryFilter = buildFilter(null, start, end, storeCode, employeeCode);
        StoreMapFilter rowsFilter = buildFilter(normalizedChannel, start, end, storeCode, employeeCode);

        List<ChannelSummary> summaries = jdbcTemplate.query(
                """
                SELECT
                    base.channel,
                    COUNT(*) AS issue_count,
                    COUNT(DISTINCT base.store_code) AS problematic_store_count
                FROM (
                """ + BASE_SQL + """
                ) base
                WHERE
                """ + summaryFilter.whereClause() + """
                GROUP BY base.channel
                """,
                (rs, rowNum) -> new ChannelSummary(
                        rs.getString("channel"),
                        rs.getLong("issue_count"),
                        rs.getLong("problematic_store_count")
                ),
                summaryFilter.params().toArray()
        );

        Long totalRows = jdbcTemplate.queryForObject(
                """
                SELECT COUNT(*) FROM (
                    SELECT
                        base.employee_code,
                        base.store_code,
                        base.channel,
                        base.visit_date
                    FROM (
                """ + BASE_SQL + """
                    ) base
                    WHERE
                """ + rowsFilter.whereClause() + """
                    GROUP BY
                        base.employee_code,
                        base.store_code,
                        base.channel,
                        base.visit_date
                ) grouped_rows
                """,
                Long.class,
                rowsFilter.params().toArray()
        );

        List<Object> rowParams = new ArrayList<>(rowsFilter.params());
        rowParams.add(safeLimit);

        List<ProblematicStoreRow> rows = jdbcTemplate.query(
                """
                SELECT
                    base.employee_code,
                    MAX(base.merchandiser_name) AS merchandiser_name,
                    base.store_code,
                    MAX(base.store_name) AS store_name,
                    MAX(base.store_format) AS store_format,
                    base.channel,
                    base.visit_date,
                    MAX(base.latitude) AS latitude,
                    MAX(base.longitude) AS longitude,
                    SUM(CASE WHEN base.rule_code = '""" + OSA_RULE + "' THEN 1 ELSE 0 END) AS osa_issue_count,\n" +
                "                    SUM(CASE WHEN base.rule_code = '" + GPS_RULE + "' THEN 1 ELSE 0 END) AS gps_issue_count,\n" +
                """
                    COUNT(*) AS total_issue_count,
                    CASE
                        WHEN base.channel = 'MT'
                            AND SUM(CASE WHEN base.rule_code = '""" + OSA_RULE + "' THEN 1 ELSE 0 END) > 0\n" +
                "                            AND COUNT(DISTINCT base.rule_code) = 1 THEN 'OSA'\n" +
                "                        WHEN base.channel = 'GT'\n" +
                "                            AND SUM(CASE WHEN base.rule_code = '" + GPS_RULE + "' THEN 1 ELSE 0 END) > 0\n" +
                "                            AND COUNT(DISTINCT base.rule_code) = 1 THEN 'GPS'\n" +
                """
                        WHEN COUNT(DISTINCT base.rule_code) = 1 THEN
                            MAX(CASE base.rule_code
                                WHEN '""" + OSA_RULE + "' THEN 'OSA unusual Non'\n" +
                "                                WHEN '" + GPS_RULE + "' THEN 'GPS monthly consistency'\n" +
                """
                                ELSE base.rule_code
                            END)
                        ELSE GROUP_CONCAT(DISTINCT CASE base.rule_code
                            WHEN '""" + OSA_RULE + "' THEN 'OSA unusual Non'\n" +
                "                            WHEN '" + GPS_RULE + "' THEN 'GPS monthly consistency'\n" +
                """
                            ELSE base.rule_code
                        END ORDER BY base.rule_code SEPARATOR ' / ')
                    END AS main_issue_type
                FROM (
                """ + BASE_SQL + """
                ) base
                WHERE
                """ + rowsFilter.whereClause() + """
                GROUP BY
                    base.employee_code,
                    base.store_code,
                    base.channel,
                    base.visit_date
                ORDER BY
                    total_issue_count DESC,
                    base.visit_date DESC,
                    store_name
                LIMIT ?
                """,
                (rs, rowNum) -> mapRow(rs),
                rowParams.toArray()
        );

        return new StoreMapProblematicStoresResponse(
                totalRows == null ? 0L : totalRows,
                findSummary(summaries, "MT"),
                findSummary(summaries, "GT"),
                rows
        );
    }

    private static StoreMapFilter buildFilter(
            String channel,
            LocalDate start,
            LocalDate end,
            String storeCode,
            String employeeCode
    ) {
        List<String> conditions = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        conditions.add("base.channel IN ('MT', 'GT')");
        conditions.add("base.run_id = (SELECT MAX(run_id) FROM validation_run_log)");
        conditions.add("""
                (
                    (base.channel = 'MT' AND base.rule_code = ?)
                    OR (base.channel = 'GT' AND base.rule_code = ?)
                )
                """);
        params.add(OSA_RULE);
        params.add(GPS_RULE);

        if (channel != null) {
            conditions.add("base.channel = ?");
            params.add(channel);
        }

        if (start != null) {
            conditions.add("base.visit_date >= ?");
            params.add(Date.valueOf(start));
        }

        if (end != null) {
            conditions.add("base.visit_date <= ?");
            params.add(Date.valueOf(end));
        }

        addEqualsFilter(conditions, params, "base.store_code", storeCode);
        addEqualsFilter(conditions, params, "base.employee_code", employeeCode);

        return new StoreMapFilter(String.join("\n  AND ", conditions), params);
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

    private static ProblematicStoreRow mapRow(ResultSet rs) throws SQLException {
        String employeeCode = rs.getString("employee_code");
        String merchandiserName = trimToNull(rs.getString("merchandiser_name"));
        if (merchandiserName == null) {
            merchandiserName = employeeCode;
        }

        return new ProblematicStoreRow(
                employeeCode,
                merchandiserName,
                rs.getString("store_code"),
                rs.getString("store_name"),
                rs.getString("store_format"),
                rs.getString("channel"),
                toLocalDate(rs.getDate("visit_date")),
                getNullableDouble(rs, "latitude"),
                getNullableDouble(rs, "longitude"),
                rs.getLong("osa_issue_count"),
                rs.getLong("gps_issue_count"),
                rs.getLong("total_issue_count"),
                rs.getString("main_issue_type")
        );
    }

    private static ChannelSummary findSummary(List<ChannelSummary> summaries, String channel) {
        return summaries.stream()
                .filter(summary -> channel.equals(summary.channel()))
                .findFirst()
                .orElseGet(() -> new ChannelSummary(channel, 0, 0));
    }

    private static String normalizeChannel(String value) {
        String normalized = trimToNull(value);
        if (normalized == null || "ALL".equalsIgnoreCase(normalized)) {
            return null;
        }

        String upper = normalized.toUpperCase();
        if (!"MT".equals(upper) && !"GT".equals(upper)) {
            throw new IllegalArgumentException("channel must be ALL, MT, or GT.");
        }
        return upper;
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

    private static LocalDate toLocalDate(Date date) {
        return date == null ? null : date.toLocalDate();
    }

    private static Double getNullableDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }

    private record StoreMapFilter(String whereClause, List<Object> params) {
    }
}
