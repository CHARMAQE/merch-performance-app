package com.smollan.backend.service;

import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.smollan.backend.dto.map.StoreMapDetailResponse;
import com.smollan.backend.dto.map.StoreMapMarkerResponse;
import com.smollan.backend.dto.mobile.MobileDashboardOverviewResponse;
import com.smollan.backend.dto.mobile.MobileLoginRequest;
import com.smollan.backend.dto.mobile.MobileSupervisorResponse;

@Service
public class MobileService {

    private final JdbcTemplate jdbcTemplate;
    private final StoreMapService storeMapService;

    public MobileService(JdbcTemplate jdbcTemplate, StoreMapService storeMapService) {
        this.jdbcTemplate = jdbcTemplate;
        this.storeMapService = storeMapService;
    }

    public Optional<MobileSupervisorResponse> login(MobileLoginRequest request) {
        if (request == null || isBlank(request.username()) || isBlank(request.password())) {
            return Optional.empty();
        }

        List<MobileSupervisorResponse> supervisors = jdbcTemplate.query(
                """
                SELECT
                    supervisor_id,
                    full_name,
                    username,
                    phone,
                    email,
                    region,
                    CASE
                        WHEN LOWER(TRIM(username)) = 'admin' THEN 'ADMIN'
                        ELSE 'SUPERVISOR'
                    END AS role
                FROM supervisors
                WHERE username = ?
                  AND password_hash = ?
                  AND active = TRUE
                LIMIT 1
                """,
                (rs, rowNum) -> new MobileSupervisorResponse(
                        rs.getLong("supervisor_id"),
                        rs.getString("full_name"),
                        rs.getString("username"),
                        rs.getString("phone"),
                        rs.getString("email"),
                        rs.getString("region"),
                        rs.getString("role")
                ),
                request.username().trim(),
                request.password().trim()
        );

        return supervisors.stream().findFirst();
    }

    public List<StoreMapMarkerResponse> getSupervisorStores(Long supervisorId) {
        if (supervisorId == null) {
            return List.of();
        }

        if (isAdmin(supervisorId)) {
            return storeMapService.getStoreMarkers();
        }

        List<StoreMapMarkerResponse> allMarkers = storeMapService.getStoreMarkers();
        List<String> assignedStoreCodes = getAssignedStoreCodes(supervisorId);

        if (assignedStoreCodes.isEmpty()) {
            return List.of();
        }

        return allMarkers.stream()
                .filter(marker -> assignedStoreCodes.contains(marker.storeCode()))
                .toList();
    }

    public MobileDashboardOverviewResponse getSupervisorOverview(
            Long supervisorId,
            Integer year,
            Integer month,
            Integer day
    ) {
        if (supervisorId == null) {
            return emptyOverview();
        }

        DateFilter dateFilter = buildDateFilter(year, month, day);
        boolean admin = isAdmin(supervisorId);

        if (admin) {
            return getAdminOverview(dateFilter);
        }

        Long stores = countAssignedStores(supervisorId);
        Long employees = countForSupervisor("""
                SELECT COUNT(DISTINCT v.employee_id) AS total_count
                FROM visits v
                """, supervisorId, dateFilter);
        Long visits = countForSupervisor("""
                SELECT COUNT(*) AS total_count
                FROM visits v
                """, supervisorId, dateFilter);
        Long surveyResponses = countForSupervisor("""
                SELECT COUNT(*) AS total_count
                FROM survey_responses sr
                JOIN visits v ON v.visit_id = sr.visit_id
                """, supervisorId, dateFilter);
        Long products = countForSupervisor("""
                SELECT COUNT(DISTINCT sr.product_code) AS total_count
                FROM survey_responses sr
                JOIN visits v ON v.visit_id = sr.visit_id
                """, supervisorId, dateFilter);
        Long visitedStores = countForSupervisor("""
                SELECT COUNT(DISTINCT v.store_id) AS total_count
                FROM visits v
                """, supervisorId, dateFilter);
        Long notVisitedStores = Math.max(0L, stores - visitedStores);
        java.time.LocalDate latestVisitDate = getLatestVisitDate(supervisorId, dateFilter);
        Long storesRevisited = countStoresRevisited(supervisorId, dateFilter);

        return new MobileDashboardOverviewResponse(
                new MobileDashboardOverviewResponse.TableCounts(
                        employees,
                        stores,
                        products,
                        visits,
                        surveyResponses
                ),
                new MobileDashboardOverviewResponse.StoreActivity(
                        stores,
                        visitedStores,
                        notVisitedStores,
                        visits,
                        latestVisitDate
                ),
                new MobileDashboardOverviewResponse.DailyReport(
                        employees,
                        storesRevisited
                )
        );
    }

    public Optional<StoreMapDetailResponse> getSupervisorStoreDetails(Long supervisorId, String storeCode) {
        if (supervisorId == null || isBlank(storeCode)) {
            return Optional.empty();
        }

        if (isAdmin(supervisorId)) {
            return Optional.of(storeMapService.getStoreDetails(storeCode.trim(), null, null));
        }

        if (!isStoreAssignedToSupervisor(supervisorId, storeCode)) {
            return Optional.empty();
        }

        return Optional.of(storeMapService.getStoreDetails(storeCode.trim(), null, null));
    }

    private MobileDashboardOverviewResponse emptyOverview() {
        return new MobileDashboardOverviewResponse(
                new MobileDashboardOverviewResponse.TableCounts(0L, 0L, 0L, 0L, 0L),
                new MobileDashboardOverviewResponse.StoreActivity(0L, 0L, 0L, 0L, null),
                new MobileDashboardOverviewResponse.DailyReport(0L, 0L)
        );
    }

    private MobileDashboardOverviewResponse getAdminOverview(DateFilter dateFilter) {
        Long stores = countAllStores();
        Long employees = countForAll("""
                SELECT COUNT(DISTINCT v.employee_id) AS total_count
                FROM visits v
                """, dateFilter);
        Long visits = countForAll("""
                SELECT COUNT(*) AS total_count
                FROM visits v
                """, dateFilter);
        Long surveyResponses = countForAll("""
                SELECT COUNT(*) AS total_count
                FROM survey_responses sr
                JOIN visits v ON v.visit_id = sr.visit_id
                """, dateFilter);
        Long products = countForAll("""
                SELECT COUNT(DISTINCT sr.product_code) AS total_count
                FROM survey_responses sr
                JOIN visits v ON v.visit_id = sr.visit_id
                """, dateFilter);
        Long visitedStores = countForAll("""
                SELECT COUNT(DISTINCT v.store_id) AS total_count
                FROM visits v
                """, dateFilter);
        Long notVisitedStores = Math.max(0L, stores - visitedStores);
        java.time.LocalDate latestVisitDate = getLatestVisitDateForAll(dateFilter);
        Long storesRevisited = countStoresRevisitedForAll(dateFilter);

        return new MobileDashboardOverviewResponse(
                new MobileDashboardOverviewResponse.TableCounts(
                        employees,
                        stores,
                        products,
                        visits,
                        surveyResponses
                ),
                new MobileDashboardOverviewResponse.StoreActivity(
                        stores,
                        visitedStores,
                        notVisitedStores,
                        visits,
                        latestVisitDate
                ),
                new MobileDashboardOverviewResponse.DailyReport(
                        employees,
                        storesRevisited
                )
        );
    }

    private Long countAssignedStores(Long supervisorId) {
        Long count = jdbcTemplate.query(
                """
                SELECT COUNT(DISTINCT ss.store_id) AS total_count
                FROM supervisor_stores ss
                JOIN supervisors sup ON sup.supervisor_id = ss.supervisor_id
                WHERE ss.supervisor_id = ?
                  AND sup.active = TRUE
                """,
                rs -> rs.next() ? rs.getLong("total_count") : 0L,
                supervisorId
        );

        return count == null ? 0L : count;
    }

    private Long countAllStores() {
        Long count = jdbcTemplate.query(
                """
                SELECT COUNT(DISTINCT store_code) AS total_count
                FROM stores
                WHERE store_code IS NOT NULL
                  AND TRIM(store_code) <> ''
                """,
                rs -> rs.next() ? rs.getLong("total_count") : 0L
        );

        return count == null ? 0L : count;
    }

    private Long countForSupervisor(String baseSql, Long supervisorId, DateFilter dateFilter) {
        List<Object> params = new java.util.ArrayList<>();
        params.add(supervisorId);
        params.addAll(dateFilter.params());

        Long count = jdbcTemplate.query(
                baseSql + """
                JOIN supervisor_stores ss ON ss.store_id = v.store_id
                JOIN supervisors sup ON sup.supervisor_id = ss.supervisor_id
                WHERE ss.supervisor_id = ?
                  AND sup.active = TRUE
                """ + dateFilter.andClause() + "\n",
                rs -> rs.next() ? rs.getLong("total_count") : 0L,
                params.toArray()
        );

        return count == null ? 0L : count;
    }

    private Long countForAll(String baseSql, DateFilter dateFilter) {
        Long count = jdbcTemplate.query(
                baseSql + """
                WHERE 1 = 1
                """ + dateFilter.andClause() + "\n",
                rs -> rs.next() ? rs.getLong("total_count") : 0L,
                dateFilter.params().toArray()
        );

        return count == null ? 0L : count;
    }

    private java.time.LocalDate getLatestVisitDate(Long supervisorId, DateFilter dateFilter) {
        List<Object> params = new java.util.ArrayList<>();
        params.add(supervisorId);
        params.addAll(dateFilter.params());

        return jdbcTemplate.query(
                """
                SELECT MAX(v.visit_date) AS latest_visit_date
                FROM visits v
                JOIN supervisor_stores ss ON ss.store_id = v.store_id
                JOIN supervisors sup ON sup.supervisor_id = ss.supervisor_id
                WHERE ss.supervisor_id = ?
                  AND sup.active = TRUE
                """ + dateFilter.andClause() + "\n",
                rs -> {
                    if (!rs.next()) {
                        return null;
                    }

                    java.sql.Date latestVisitDate = rs.getDate("latest_visit_date");
                    return latestVisitDate == null ? null : latestVisitDate.toLocalDate();
                },
                params.toArray()
        );
    }

    private java.time.LocalDate getLatestVisitDateForAll(DateFilter dateFilter) {
        return jdbcTemplate.query(
                """
                SELECT MAX(v.visit_date) AS latest_visit_date
                FROM visits v
                WHERE 1 = 1
                """ + dateFilter.andClause() + "\n",
                rs -> {
                    if (!rs.next()) {
                        return null;
                    }

                    java.sql.Date latestVisitDate = rs.getDate("latest_visit_date");
                    return latestVisitDate == null ? null : latestVisitDate.toLocalDate();
                },
                dateFilter.params().toArray()
        );
    }

    private Long countStoresRevisited(Long supervisorId, DateFilter dateFilter) {
        List<Object> params = new java.util.ArrayList<>();
        params.add(supervisorId);
        params.addAll(dateFilter.params());

        Long count = jdbcTemplate.query(
                """
                SELECT COUNT(*) AS total_count
                FROM (
                    SELECT s.store_code
                    FROM visits v
                    JOIN stores s ON s.store_id = v.store_id
                    JOIN employees e ON e.employee_id = v.employee_id
                    JOIN supervisor_stores ss ON ss.store_id = v.store_id
                    JOIN supervisors sup ON sup.supervisor_id = ss.supervisor_id
                    WHERE ss.supervisor_id = ?
                      AND sup.active = TRUE
                    """ + dateFilter.andClause() + """

                    GROUP BY s.store_code
                    HAVING COUNT(DISTINCT e.employee_code) > 1
                ) revisited_stores
                """,
                rs -> rs.next() ? rs.getLong("total_count") : 0L,
                params.toArray()
        );

        return count == null ? 0L : count;
    }

    private Long countStoresRevisitedForAll(DateFilter dateFilter) {
        Long count = jdbcTemplate.query(
                """
                SELECT COUNT(*) AS total_count
                FROM (
                    SELECT s.store_code
                    FROM visits v
                    JOIN stores s ON s.store_id = v.store_id
                    JOIN employees e ON e.employee_id = v.employee_id
                    WHERE 1 = 1
                    """ + dateFilter.andClause() + """

                    GROUP BY s.store_code
                    HAVING COUNT(DISTINCT e.employee_code) > 1
                ) revisited_stores
                """,
                rs -> rs.next() ? rs.getLong("total_count") : 0L,
                dateFilter.params().toArray()
        );

        return count == null ? 0L : count;
    }

    private List<String> getAssignedStoreCodes(Long supervisorId) {
        return jdbcTemplate.query(
                """
                SELECT s.store_code
                FROM supervisor_stores ss
                JOIN stores s ON s.store_id = ss.store_id
                JOIN supervisors sup ON sup.supervisor_id = ss.supervisor_id
                WHERE ss.supervisor_id = ?
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
                  AND sup.active = TRUE
                """,
                rs -> rs.next() ? rs.getInt("assignment_count") : 0,
                supervisorId,
                storeCode.trim()
        );

        return count != null && count > 0;
    }

    private boolean isAdmin(Long supervisorId) {
        if (supervisorId == null) {
            return false;
        }

        Integer count = jdbcTemplate.query(
                """
                SELECT COUNT(*) AS admin_count
                FROM supervisors
                WHERE supervisor_id = ?
                  AND LOWER(TRIM(username)) = 'admin'
                  AND active = TRUE
                """,
                rs -> rs.next() ? rs.getInt("admin_count") : 0,
                supervisorId
        );

        return count != null && count > 0;
    }

    private static DateFilter buildDateFilter(Integer year, Integer month, Integer day) {
        List<String> conditions = new java.util.ArrayList<>();
        List<Object> params = new java.util.ArrayList<>();

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
            return new DateFilter("", List.of());
        }

        return new DateFilter(" AND " + String.join(" AND ", conditions), params);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private record DateFilter(String andClause, List<Object> params) {
    }

}
