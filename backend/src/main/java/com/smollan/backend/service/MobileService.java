package com.smollan.backend.service;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.bcrypt.BCrypt;
import org.springframework.stereotype.Service;

import com.smollan.backend.dto.map.StoreMapDetailResponse;
import com.smollan.backend.dto.mobile.MobileDashboardOverviewResponse;
import com.smollan.backend.dto.mobile.MobileExecutionStoreSummaryResponse;
import com.smollan.backend.dto.mobile.MobileLoginRequest;
import com.smollan.backend.dto.mobile.MobileMerchandiserExecutionResponse;
import com.smollan.backend.dto.mobile.MobileMerchandiserStoreResponse;
import com.smollan.backend.dto.mobile.MobileStoreCoverageResponse;
import com.smollan.backend.dto.mobile.MobileStoreDetailResponse;
import com.smollan.backend.dto.mobile.MobileSupervisorResponse;

@Service
public class MobileService {

    private static final int GPS_DISTANCE_THRESHOLD_METERS = 250;
    private static final String NON_VISITED_CONDITION = "(fc.is_done = 0 AND fc.rejection = 0 AND fc.deviation = 0)";
    private static final String COVERED_VISIT_CONDITION = "(fc.is_done = 1 AND fc.rejection = 0 AND fc.deviation = 0)";
    private static final String GPS_DISTANCE_CONDITION =
            "(COALESCE(fc.start_distance_meters, fc.end_distance_meters) > " + GPS_DISTANCE_THRESHOLD_METERS + ")";
    private static final String LOW_TASK_COMPLETION_CONDITION =
            "(" + COVERED_VISIT_CONDITION
                    + " AND COALESCE(fc.task_assigned, 0) > 0"
                    + " AND COALESCE(fc.task_done, 0) < COALESCE(fc.task_assigned, 0))";
    private static final String FOLLOW_UP_CONDITION =
            "(" + NON_VISITED_CONDITION
                    + " OR fc.rejection = 1"
                    + " OR fc.deviation = 1"
                    + " OR " + GPS_DISTANCE_CONDITION
                    + " OR " + LOW_TASK_COMPLETION_CONDITION
                    + ")";

    private final JdbcTemplate jdbcTemplate;
    private final StoreMapService storeMapService;

    public MobileService(JdbcTemplate jdbcTemplate, StoreMapService storeMapService) {
        this.jdbcTemplate = jdbcTemplate;
        this.storeMapService = storeMapService;
    }

    public Optional<MobileSupervisorResponse> login(MobileLoginRequest request) {
        if (request == null || isBlank(request.email()) || isBlank(request.password())) {
            return Optional.empty();
        }

        List<SupervisorAccount> supervisors = jdbcTemplate.query(
                """
                SELECT
                    sup.supervisor_id,
                    sup.supervisor_code,
                    sup.full_name,
                    sup.username,
                    sup.password_hash,
                    sup.email,
                    sup.city,
                    sup.region,
                    CASE
                        WHEN LOWER(TRIM(sup.username)) = 'admin' THEN 'ADMIN'
                        ELSE UPPER(COALESCE(NULLIF(TRIM(sup.role), ''), 'CLIENT_SUPERVISOR'))
                    END AS role,
                    (
                        SELECT COUNT(*)
                        FROM supervisor_stores ss
                        WHERE ss.supervisor_id = sup.supervisor_id
                          AND ss.active = TRUE
                    ) AS assigned_store_count
                FROM supervisors sup
                WHERE LOWER(TRIM(sup.email)) = ?
                  AND sup.active = TRUE
                LIMIT 1
                """,
                (rs, rowNum) -> new SupervisorAccount(
                        rs.getLong("supervisor_id"),
                        rs.getString("supervisor_code"),
                        rs.getString("full_name"),
                        rs.getString("username"),
                        rs.getString("password_hash"),
                        rs.getString("email"),
                        rs.getString("city"),
                        rs.getString("region"),
                        rs.getString("role"),
                        rs.getLong("assigned_store_count")
                ),
                request.email().trim().toLowerCase()
        );

        if (supervisors.isEmpty()) {
            return Optional.empty();
        }

        SupervisorAccount supervisor = supervisors.get(0);
        if (!passwordMatches(request.password().trim(), supervisor.passwordHash())) {
            return Optional.empty();
        }

        return Optional.of(new MobileSupervisorResponse(
                supervisor.supervisorId(),
                supervisor.supervisorCode(),
                supervisor.fullName(),
                supervisor.username(),
                supervisor.email(),
                supervisor.city(),
                supervisor.region(),
                supervisor.role(),
                supervisor.assignedStoreCount()
        ));
    }

    public MobileDashboardOverviewResponse getSupervisorOverview(
            Long supervisorId,
            String startDate,
            String endDate,
            String supervisor,
            String merchandiser,
            String region,
            String storeFormat,
            Integer year,
            Integer month,
            Integer day
    ) {
        CoverageFilter filter = buildCoverageFilter(
                supervisorId, startDate, endDate, supervisor, merchandiser, region, storeFormat, year, month, day
        );

        return jdbcTemplate.query(
                """
                SELECT
                    COALESCE(SUM(fc.is_planned = 1), 0) AS planned_visits,
                    COALESCE(SUM(fc.is_done = 1), 0) AS executed_visits,
                    COALESCE(SUM(""" + NON_VISITED_CONDITION + """
                    ), 0) AS non_visited_visits,
                    COALESCE(SUM(fc.deviation = 1), 0) AS deviation_visits,
                    COALESCE(SUM(fc.rejection = 1), 0) AS rejected_visits,
                    COALESCE(SUM(""" + FOLLOW_UP_CONDITION + """
                    ), 0) AS problematic_visits,
                    COALESCE(COUNT(DISTINCT fc.employee_code), 0) AS active_merchandisers,
                    COALESCE(COUNT(DISTINCT fc.store_code), 0) AS stores,
                    COALESCE(COUNT(DISTINCT CASE WHEN """ + COVERED_VISIT_CONDITION + """
                     THEN fc.store_code END), 0) AS covered_stores,
                    COALESCE(SUM(CASE WHEN """ + COVERED_VISIT_CONDITION + """
                     AND COALESCE(fc.task_assigned, 0) > 0 THEN COALESCE(fc.task_done, 0) ELSE 0 END), 0) AS task_done,
                    COALESCE(SUM(CASE WHEN """ + COVERED_VISIT_CONDITION + """
                     AND COALESCE(fc.task_assigned, 0) > 0 THEN COALESCE(fc.task_assigned, 0) ELSE 0 END), 0) AS task_assigned,
                    MAX(fc.visit_date) AS latest_visit_date
                FROM fact_coverage fc
                WHERE 1 = 1
                """ + filter.andClause(),
                rs -> {
                    if (!rs.next()) {
                        return emptyOverview();
                    }

                    long plannedVisits = rs.getLong("planned_visits");
                    long executedVisits = rs.getLong("executed_visits");
                    long nonVisitedVisits = rs.getLong("non_visited_visits");
                    long deviationVisits = rs.getLong("deviation_visits");
                    long rejectedVisits = rs.getLong("rejected_visits");
                    long problematicVisits = rs.getLong("problematic_visits");
                    long stores = rs.getLong("stores");
                    long coveredStores = rs.getLong("covered_stores");
                    long activeMerchandisers = rs.getLong("active_merchandisers");
                    long taskDone = rs.getLong("task_done");
                    long taskAssigned = rs.getLong("task_assigned");
                    java.sql.Date latestVisitDate = rs.getDate("latest_visit_date");
                    LocalDate latestVisitDateValue = latestVisitDate == null ? null : latestVisitDate.toLocalDate();

                    return new MobileDashboardOverviewResponse(
                            plannedVisits,
                            executedVisits,
                            nonVisitedVisits,
                            deviationVisits,
                            rejectedVisits,
                            problematicVisits,
                            rate(executedVisits, plannedVisits),
                            rate(nonVisitedVisits, plannedVisits),
                            rate(deviationVisits, plannedVisits),
                            rate(rejectedVisits, plannedVisits),
                            rate(taskDone, taskAssigned),
                            latestVisitDateValue,
                            activeMerchandisers,
                            new MobileDashboardOverviewResponse.TableCounts(
                                    activeMerchandisers,
                                    stores,
                                    0L,
                                    executedVisits,
                                    0L
                            ),
                            new MobileDashboardOverviewResponse.StoreActivity(
                                    stores,
                                    coveredStores,
                                    nonVisitedVisits,
                                    executedVisits,
                                    latestVisitDateValue,
                                    plannedVisits,
                                    executedVisits,
                                    deviationVisits
                            ),
                            new MobileDashboardOverviewResponse.DailyReport(
                                    activeMerchandisers,
                                    Math.max(0L, executedVisits - coveredStores)
                            )
                    );
                },
                filter.params().toArray()
        );
    }

    public List<MobileMerchandiserExecutionResponse> getMerchandiserExecution(
            Long supervisorId,
            String startDate,
            String endDate,
            String supervisor,
            String merchandiser,
            String region,
            String storeFormat,
            String storeFormatGroup,
            Integer year,
            Integer month,
            Integer day
    ) {
        CoverageFilter filter = buildCoverageFilter(
                supervisorId, startDate, endDate, supervisor, merchandiser, region, storeFormat, year, month, day
        );
        String storeFormatGroupClause = buildStoreFormatGroupClause(storeFormatGroup);

        return jdbcTemplate.query(
                """
                SELECT
                    fc.employee_code,
                    MAX(fc.username) AS username,
                    MAX(COALESCE(fc.l1name, fc.l2name, fc.l3name)) AS supervisor_name,
                    MAX(fc.store_region) AS region,
                    MAX(fc.store_city) AS city,
                    COALESCE(SUM(fc.is_planned = 1), 0) AS planned_visits,
                    COALESCE(SUM(fc.is_adhoc = 1), 0) AS adhoc_visits,
                    COALESCE(SUM(fc.is_done = 1), 0) AS executed_visits,
                    COALESCE(SUM(""" + NON_VISITED_CONDITION + """
                    ), 0) AS non_visited_visits,
                    COALESCE(SUM(fc.deviation = 1), 0) AS deviation_visits,
                    COALESCE(SUM(fc.rejection = 1), 0) AS rejected_visits,
                    COALESCE(SUM(""" + FOLLOW_UP_CONDITION + """
                    ), 0) AS problematic_visits,
                    COALESCE(SUM(CASE WHEN """ + COVERED_VISIT_CONDITION + """
                     AND COALESCE(fc.task_assigned, 0) > 0 THEN COALESCE(fc.task_done, 0) ELSE 0 END), 0) AS task_done,
                    COALESCE(SUM(CASE WHEN """ + COVERED_VISIT_CONDITION + """
                     AND COALESCE(fc.task_assigned, 0) > 0 THEN COALESCE(fc.task_assigned, 0) ELSE 0 END), 0) AS task_assigned,
                    AVG(fc.time_mm) AS avg_visit_duration,
                    AVG(COALESCE(fc.start_distance_meters, fc.end_distance_meters)) AS avg_distance_from_store,
                    COALESCE(COUNT(DISTINCT CASE WHEN """ + COVERED_VISIT_CONDITION + """
                     THEN fc.store_code END), 0) AS stores_covered,
                    GROUP_CONCAT(DISTINCT fc.store_city ORDER BY fc.store_city SEPARATOR ', ') AS cities,
                    GROUP_CONCAT(DISTINCT fc.store_format ORDER BY fc.store_format SEPARATOR ', ') AS store_formats,
                    MAX(fc.visit_date) AS latest_visit_date
                FROM fact_coverage fc
                WHERE 1 = 1
                """ + filter.andClause() + storeFormatGroupClause + """

                GROUP BY fc.employee_code
                ORDER BY planned_visits DESC, executed_visits DESC, fc.employee_code
                LIMIT 100
                """,
                (rs, rowNum) -> mapMerchandiserExecution(rs),
                filter.params().toArray()
        );
    }

    public List<MobileStoreCoverageResponse> getCoverageStores(
            Long supervisorId,
            String startDate,
            String endDate,
            String supervisor,
            String merchandiser,
            String region,
            String storeFormat,
            Integer year,
            Integer month,
            Integer day
    ) {
        CoverageFilter filter = buildCoverageFilter(
                supervisorId, startDate, endDate, supervisor, merchandiser, region, storeFormat, year, month, day
        );

        return jdbcTemplate.query(
                """
                SELECT
                    fc.store_code,
                    fc.store_name,
                    fc.store_city,
                    fc.store_region,
                    fc.store_format,
                    fc.employee_code,
                    fc.username,
                    COALESCE(fc.l1name, fc.l2name, fc.l3name) AS supervisor_name,
                    fc.visit_date,
                    fc.call_status,
                    fc.call_cycle_type,
                    fc.is_planned,
                    fc.is_done,
                    """ + NON_VISITED_CONDITION + """
                     AS not_visited,
                    fc.deviation,
                    fc.rejection,
                    fc.task_assigned,
                    fc.task_done,
                    fc.task_per,
                    fc.reason,
                    fc.master_latitude,
                    fc.master_longitude,
                    fc.start_distance_meters,
                    fc.end_distance_meters
                FROM fact_coverage fc
                WHERE 1 = 1
                """ + filter.andClause() + """

                ORDER BY fc.visit_date DESC, fc.store_name, fc.store_code
                LIMIT 500
                """,
                (rs, rowNum) -> mapCoverageStore(rs),
                filter.params().toArray()
        );
    }

    public List<MobileMerchandiserStoreResponse> getMerchandiserStores(
            Long supervisorId,
            String employeeCode,
            String startDate,
            String endDate,
            String storeFormatGroup,
            Integer year,
            Integer month,
            Integer day
    ) {
        if (isBlank(employeeCode)) {
            return List.of();
        }

        CoverageFilter filter = buildCoverageFilter(
                supervisorId, startDate, endDate, null, employeeCode, null, null, year, month, day
        );
        String storeFormatGroupClause = buildStoreFormatGroupClause(storeFormatGroup);

        return jdbcTemplate.query(
                """
                SELECT
                    fc.store_code,
                    fc.store_name,
                    fc.store_city,
                    fc.store_region,
                    fc.store_format,
                    fc.visit_date,
                    fc.is_planned,
                    fc.is_adhoc,
                    fc.call_cycle_type,
                    fc.deviation,
                    """ + NON_VISITED_CONDITION + """
                     AS not_visited,
                    fc.rejection,
                    fc.task_assigned,
                    fc.task_done,
                    fc.task_per,
                    fc.reason
                FROM fact_coverage fc
                WHERE 1 = 1
                """ + filter.andClause() + storeFormatGroupClause + """

                ORDER BY fc.visit_date DESC, fc.store_name, fc.store_code
                LIMIT 300
                """,
                (rs, rowNum) -> {
                    String status = "Covered";
                    if (rs.getBoolean("not_visited")) {
                        status = "Non Visited";
                    } else if (rs.getBoolean("rejection")) {
                        status = "Rejected";
                    } else if (rs.getBoolean("deviation")) {
                        status = "Deviation";
                    }

                    return new MobileMerchandiserStoreResponse(
                            rs.getString("store_code"),
                            rs.getString("store_name"),
                            rs.getString("store_city"),
                            rs.getString("store_region"),
                            rs.getString("store_format"),
                            toLocalDate(rs, "visit_date"),
                            status,
                            rs.getBoolean("is_planned"),
                            rs.getBoolean("is_adhoc"),
                            rs.getString("call_cycle_type"),
                            rs.getString("reason"),
                            getNullableInteger(rs, "task_assigned"),
                            getNullableInteger(rs, "task_done"),
                            getNullableDouble(rs, "task_per")
                    );
                },
                filter.params().toArray()
        );
    }

    public List<MobileExecutionStoreSummaryResponse> getExecutionStores(
            Long supervisorId,
            String type,
            Integer year,
            Integer month,
            Integer day
    ) {
        if (isBlank(type)) {
            return List.of();
        }

        String normalizedType = type.trim().toLowerCase();
        String statusCondition;
        if ("covered".equals(normalizedType)) {
            statusCondition = " AND " + COVERED_VISIT_CONDITION;
        } else if ("deviation".equals(normalizedType) || "deviations".equals(normalizedType)) {
            statusCondition = " AND fc.deviation = 1";
        } else {
            return List.of();
        }

        CoverageFilter filter = buildCoverageFilter(
                supervisorId, null, null, null, null, null, null, year, month, day
        );

        return jdbcTemplate.query(
                """
                SELECT
                    fc.store_code,
                    MAX(fc.store_name) AS store_name,
                    MAX(fc.store_city) AS store_city,
                    MAX(fc.store_format) AS store_format,
                    COUNT(*) AS execution_count,
                    COUNT(DISTINCT fc.employee_code) AS merchandiser_count,
                    MAX(fc.visit_date) AS latest_visit_date
                FROM fact_coverage fc
                WHERE 1 = 1
                """ + filter.andClause() + statusCondition + """

                GROUP BY fc.store_code
                ORDER BY execution_count DESC, latest_visit_date DESC, store_name
                LIMIT 100
                """,
                (rs, rowNum) -> new MobileExecutionStoreSummaryResponse(
                        rs.getString("store_code"),
                        rs.getString("store_name"),
                        rs.getString("store_city"),
                        rs.getString("store_format"),
                        rs.getLong("execution_count"),
                        rs.getLong("merchandiser_count"),
                        toLocalDate(rs, "latest_visit_date")
                ),
                filter.params().toArray()
        );
    }

    public Optional<StoreMapDetailResponse> getSupervisorStoreDetails(Long supervisorId, String storeCode) {
        if (isBlank(storeCode)) {
            return Optional.empty();
        }

        if (!canAccessStore(supervisorId, storeCode)) {
            return Optional.empty();
        }

        return Optional.of(storeMapService.getStoreDetails(storeCode.trim(), null, null));
    }

    public Optional<MobileStoreDetailResponse> getMobileStoreDetails(
            Long supervisorId,
            String storeCode,
            Integer year,
            Integer month,
            Integer day
    ) {
        if (isBlank(storeCode)) {
            return Optional.empty();
        }

        if (!canAccessStore(supervisorId, storeCode)) {
            return Optional.empty();
        }

        CoverageFilter filter = buildCoverageFilter(
                supervisorId, null, null, null, null, null, null, year, month, day
        );

        List<Object> params = new ArrayList<>();
        params.add(storeCode.trim());
        params.addAll(filter.params());

        List<MobileStoreDetailResponse> rows = jdbcTemplate.query(
                """
                SELECT
                    fc.store_code,
                    fc.store_name,
                    fc.store_city,
                    fc.store_region,
                    fc.store_format,
                    fc.employee_code,
                    fc.username,
                    COALESCE(fc.l1name, fc.l2name, fc.l3name) AS supervisor_name,
                    fc.visit_date,
                    fc.call_status,
                    fc.call_cycle_type,
                    fc.is_planned,
                    fc.is_done,
                    """ + NON_VISITED_CONDITION + """
                     AS not_visited,
                    fc.deviation,
                    fc.rejection,
                    fc.task_assigned,
                    fc.task_done,
                    fc.task_per,
                    fc.reason,
                    fc.user_attendance,
                    fc.superior_attendance,
                    fc.final_user_attendance,
                    fc.master_latitude,
                    fc.master_longitude,
                    fc.start_distance_meters,
                    fc.end_distance_meters,
                    fc.start_time,
                    fc.end_time,
                    (
                        SELECT MAX(latest.visit_date)
                        FROM fact_coverage latest
                        WHERE latest.store_code = fc.store_code
                    ) AS latest_visit_date
                FROM fact_coverage fc
                WHERE fc.store_code = ?
                """ + filter.andClause() + """

                ORDER BY fc.visit_date DESC, fc.start_time DESC
                LIMIT 1
                """,
                (rs, rowNum) -> new MobileStoreDetailResponse(
                        rs.getString("store_code"),
                        rs.getString("store_name"),
                        rs.getString("store_city"),
                        rs.getString("store_region"),
                        rs.getString("store_format"),
                        rs.getString("employee_code"),
                        rs.getString("username"),
                        rs.getString("supervisor_name"),
                        toLocalDate(rs, "visit_date"),
                        toLocalDate(rs, "latest_visit_date"),
                        rs.getString("call_status"),
                        rs.getString("call_cycle_type"),
                        rs.getBoolean("is_planned"),
                        rs.getBoolean("is_done"),
                        rs.getBoolean("not_visited"),
                        rs.getBoolean("deviation"),
                        rs.getBoolean("rejection"),
                        getNullableInteger(rs, "task_assigned"),
                        getNullableInteger(rs, "task_done"),
                        getNullableDouble(rs, "task_per"),
                        rs.getString("reason"),
                        rs.getString("user_attendance"),
                        rs.getString("superior_attendance"),
                        rs.getString("final_user_attendance"),
                        rs.getBigDecimal("master_latitude"),
                        rs.getBigDecimal("master_longitude"),
                        rs.getBigDecimal("start_distance_meters"),
                        rs.getBigDecimal("end_distance_meters"),
                        toLocalDateTime(rs, "start_time"),
                        toLocalDateTime(rs, "end_time")
                ),
                params.toArray()
        );

        return rows.stream().findFirst();
    }

    private MobileMerchandiserExecutionResponse mapMerchandiserExecution(ResultSet rs) throws SQLException {
        long plannedVisits = rs.getLong("planned_visits");
        long executedVisits = rs.getLong("executed_visits");
        long nonVisitedVisits = rs.getLong("non_visited_visits");
        long deviationVisits = rs.getLong("deviation_visits");
        long rejectedVisits = rs.getLong("rejected_visits");
        long taskDone = rs.getLong("task_done");
        long taskAssigned = rs.getLong("task_assigned");

        return new MobileMerchandiserExecutionResponse(
                rs.getString("employee_code"),
                rs.getString("username"),
                rs.getString("supervisor_name"),
                rs.getString("region"),
                rs.getString("city"),
                plannedVisits,
                rs.getLong("adhoc_visits"),
                executedVisits,
                rate(executedVisits, plannedVisits),
                nonVisitedVisits,
                deviationVisits,
                rejectedVisits,
                rs.getLong("problematic_visits"),
                rate(taskDone, taskAssigned),
                getNullableDouble(rs, "avg_visit_duration"),
                getNullableDouble(rs, "avg_distance_from_store"),
                rs.getLong("stores_covered"),
                splitCommaSeparatedValues(rs.getString("cities")),
                splitCommaSeparatedValues(rs.getString("store_formats")),
                toLocalDate(rs, "latest_visit_date")
        );
    }

    private MobileStoreCoverageResponse mapCoverageStore(ResultSet rs) throws SQLException {
        BigDecimal latitude = rs.getBigDecimal("master_latitude");
        BigDecimal longitude = rs.getBigDecimal("master_longitude");

        return new MobileStoreCoverageResponse(
                rs.getString("store_code"),
                rs.getString("store_name"),
                rs.getString("store_city"),
                rs.getString("store_city"),
                rs.getString("store_region"),
                rs.getString("store_region"),
                rs.getString("store_format"),
                rs.getString("employee_code"),
                rs.getString("username"),
                rs.getString("supervisor_name"),
                toLocalDate(rs, "visit_date"),
                rs.getString("call_status"),
                rs.getString("call_cycle_type"),
                rs.getBoolean("is_planned"),
                rs.getBoolean("is_done"),
                rs.getBoolean("not_visited"),
                rs.getBoolean("deviation"),
                rs.getBoolean("rejection"),
                getNullableInteger(rs, "task_assigned"),
                getNullableInteger(rs, "task_done"),
                getNullableDouble(rs, "task_per"),
                rs.getString("reason"),
                latitude,
                longitude,
                latitude == null ? null : latitude.doubleValue(),
                longitude == null ? null : longitude.doubleValue(),
                rs.getBigDecimal("start_distance_meters"),
                rs.getBigDecimal("end_distance_meters")
        );
    }

    private MobileDashboardOverviewResponse emptyOverview() {
        return new MobileDashboardOverviewResponse(
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                null,
                0L,
                new MobileDashboardOverviewResponse.TableCounts(0L, 0L, 0L, 0L, 0L),
                new MobileDashboardOverviewResponse.StoreActivity(0L, 0L, 0L, 0L, null, 0L, 0L, 0L),
                new MobileDashboardOverviewResponse.DailyReport(0L, 0L)
        );
    }

    private CoverageFilter buildCoverageFilter(
            Long supervisorId,
            String startDate,
            String endDate,
            String supervisor,
            String merchandiser,
            String region,
            String storeFormat,
            Integer year,
            Integer month,
            Integer day
    ) {
        List<String> conditions = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        LocalDate parsedStartDate = parseDate(startDate);
        LocalDate parsedEndDate = parseDate(endDate);

        if (parsedStartDate != null) {
            conditions.add("fc.visit_date >= ?");
            params.add(parsedStartDate);
        }
        if (parsedEndDate != null) {
            conditions.add("fc.visit_date <= ?");
            params.add(parsedEndDate);
        }

        if (parsedStartDate == null && parsedEndDate == null) {
            if (year != null) {
                conditions.add("YEAR(fc.visit_date) = ?");
                params.add(year);
            }
            if (month != null) {
                conditions.add("MONTH(fc.visit_date) = ?");
                params.add(month);
            }
            if (day != null) {
                conditions.add("DAYOFMONTH(fc.visit_date) = ?");
                params.add(day);
            }
        }

        if (!isBlank(supervisor)) {
            conditions.add("""
                    (
                        LOWER(COALESCE(fc.l1name, '')) LIKE ?
                        OR LOWER(COALESCE(fc.l2name, '')) LIKE ?
                        OR LOWER(COALESCE(fc.l3name, '')) LIKE ?
                    )
                    """);
            String value = likeValue(supervisor);
            params.add(value);
            params.add(value);
            params.add(value);
        }

        if (!isBlank(merchandiser)) {
            conditions.add("(LOWER(fc.employee_code) = ? OR LOWER(COALESCE(fc.username, '')) LIKE ?)");
            params.add(merchandiser.trim().toLowerCase());
            params.add(likeValue(merchandiser));
        }

        if (!isBlank(region)) {
            conditions.add("LOWER(COALESCE(fc.store_region, '')) LIKE ?");
            params.add(likeValue(region));
        }

        if (!isBlank(storeFormat)) {
            conditions.add("LOWER(COALESCE(fc.store_format, '')) LIKE ?");
            params.add(likeValue(storeFormat));
        }

        if (supervisorId == null || supervisorId <= 0) {
            conditions.add("1 = 0");
        } else if (!isAdmin(supervisorId)) {
            List<String> assignedStoreCodes = getAssignedStoreCodes(supervisorId);
            if (!assignedStoreCodes.isEmpty()) {
                String placeholders = String.join(",", assignedStoreCodes.stream().map(item -> "?").toList());
                conditions.add("fc.store_code IN (" + placeholders + ")");
                params.addAll(assignedStoreCodes);
            } else {
                conditions.add("1 = 0");
            }
        }

        if (conditions.isEmpty()) {
            return new CoverageFilter("", List.of());
        }

        return new CoverageFilter(" AND " + String.join(" AND ", conditions), params);
    }

    private static String buildStoreFormatGroupClause(String storeFormatGroup) {
        if (isBlank(storeFormatGroup) || "ALL".equalsIgnoreCase(storeFormatGroup.trim())) {
            return "";
        }

        // GT means Grocery. MT means every other known non-blank store format.
        if ("GT".equalsIgnoreCase(storeFormatGroup.trim())) {
            return " AND UPPER(TRIM(COALESCE(fc.store_format, ''))) = 'GROCERY'";
        }
        if ("MT".equalsIgnoreCase(storeFormatGroup.trim())) {
            return """
                     AND TRIM(COALESCE(fc.store_format, '')) <> ''
                     AND UPPER(TRIM(COALESCE(fc.store_format, ''))) <> 'GROCERY'
                    """;
        }

        return "";
    }

    private List<String> getAssignedStoreCodes(Long supervisorId) {
        if (supervisorId == null || supervisorId <= 0) {
            return List.of();
        }

        return jdbcTemplate.query(
                """
                SELECT s.store_code
                FROM supervisor_stores ss
                JOIN stores s ON s.store_id = ss.store_id
                JOIN supervisors sup ON sup.supervisor_id = ss.supervisor_id
                WHERE ss.supervisor_id = ?
                  AND ss.active = TRUE
                  AND sup.active = TRUE
                ORDER BY s.store_name, s.store_code
                """,
                (rs, rowNum) -> rs.getString("store_code"),
                supervisorId
        );
    }

    private boolean isStoreAssignedToSupervisor(Long supervisorId, String storeCode) {
        Integer count = jdbcTemplate.query(
                """
                SELECT COUNT(*) AS assignment_count
                FROM supervisor_stores ss
                JOIN stores s ON s.store_id = ss.store_id
                JOIN supervisors sup ON sup.supervisor_id = ss.supervisor_id
                WHERE ss.supervisor_id = ?
                  AND s.store_code = ?
                  AND ss.active = TRUE
                  AND sup.active = TRUE
                """,
                rs -> rs.next() ? rs.getInt("assignment_count") : 0,
                supervisorId,
                storeCode.trim()
        );

        return count != null && count > 0;
    }

    private boolean canAccessStore(Long supervisorId, String storeCode) {
        if (supervisorId == null || supervisorId <= 0 || isBlank(storeCode)) {
            return false;
        }

        if (isAdmin(supervisorId)) {
            return true;
        }

        return isStoreAssignedToSupervisor(supervisorId, storeCode);
    }

    private boolean isAdmin(Long supervisorId) {
        if (supervisorId == null || supervisorId <= 0) {
            return false;
        }

        Integer count = jdbcTemplate.query(
                """
                SELECT COUNT(*) AS admin_count
                FROM supervisors
                WHERE supervisor_id = ?
                  AND active = TRUE
                  AND (
                      LOWER(TRIM(username)) = 'admin'
                      OR UPPER(TRIM(COALESCE(role, ''))) = 'ADMIN'
                  )
                """,
                rs -> rs.next() ? rs.getInt("admin_count") : 0,
                supervisorId
        );

        return count != null && count > 0;
    }

    private static List<Object> prependAndAppendDistance(List<Object> params) {
        List<Object> out = new ArrayList<>();
        out.add(GPS_DISTANCE_THRESHOLD_METERS);
        out.addAll(params);
        out.add(GPS_DISTANCE_THRESHOLD_METERS);
        return out;
    }

    private static boolean passwordMatches(String rawPassword, String passwordHash) {
        if (isBlank(rawPassword) || isBlank(passwordHash)) {
            return false;
        }

        try {
            return BCrypt.checkpw(rawPassword, passwordHash);
        } catch (IllegalArgumentException exc) {
            return false;
        }
    }

    private static LocalDate parseDate(String value) {
        if (isBlank(value)) {
            return null;
        }

        try {
            return LocalDate.parse(value.trim());
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private static String likeValue(String value) {
        return "%" + value.trim().toLowerCase() + "%";
    }

    private static Double rate(long numerator, long denominator) {
        if (denominator <= 0) {
            return 0.0;
        }

        return (numerator * 100.0) / denominator;
    }

    private static LocalDate toLocalDate(ResultSet rs, String column) throws SQLException {
        java.sql.Date value = rs.getDate(column);
        return value == null ? null : value.toLocalDate();
    }

    private static LocalDateTime toLocalDateTime(ResultSet rs, String column) throws SQLException {
        java.sql.Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toLocalDateTime();
    }

    private static Integer getNullableInteger(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static Double getNullableDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }

    private static List<String> splitCommaSeparatedValues(String value) {
        if (isBlank(value)) {
            return List.of();
        }

        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(item -> !item.isEmpty())
                .toList();
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private record CoverageFilter(String andClause, List<Object> params) {
    }

    private record SupervisorAccount(
            Long supervisorId,
            String supervisorCode,
            String fullName,
            String username,
            String passwordHash,
            String email,
            String city,
            String region,
            String role,
            Long assignedStoreCount
    ) {
    }
}
