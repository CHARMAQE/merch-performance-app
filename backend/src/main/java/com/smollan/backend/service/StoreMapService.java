package com.smollan.backend.service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.smollan.backend.dto.map.StoreMapDetailResponse;
import com.smollan.backend.dto.map.StoreMapMarkerResponse;

@Service
public class StoreMapService {

    private static final String STORE_GPS_SQL = """
            SELECT
                s.store_code,
                s.store_name,
                s.store_city,
                s.store_state,
                s.store_region,
                s.store_format,
                v.latitude,
                v.longitude
            FROM visits v
            JOIN stores s ON s.store_id = v.store_id
            WHERE v.latitude IS NOT NULL
              AND v.longitude IS NOT NULL
              AND v.latitude <> 0
              AND v.longitude <> 0
            ORDER BY s.store_code, v.visit_date, v.visit_id
            """;

    private final JdbcTemplate jdbcTemplate;

    public StoreMapService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<StoreMapMarkerResponse> getStoreMarkers() {
        List<GpsRow> rows = jdbcTemplate.query(
                STORE_GPS_SQL,
                (rs, rowNum) -> new GpsRow(
                        rs.getString("store_code"),
                        rs.getString("store_name"),
                        rs.getString("store_city"),
                        rs.getString("store_state"),
                        rs.getString("store_region"),
                        rs.getString("store_format"),
                        rs.getDouble("latitude"),
                        rs.getDouble("longitude")
                )
        );

        Map<String, List<GpsRow>> groupedByStoreCode = new LinkedHashMap<>();
        for (GpsRow row : rows) {
            groupedByStoreCode
                    .computeIfAbsent(row.storeCode(), key -> new ArrayList<>())
                    .add(row);
        }

        List<StoreMapMarkerResponse> markers = new ArrayList<>();

        for (Map.Entry<String, List<GpsRow>> entry : groupedByStoreCode.entrySet()) {
            String storeCode = entry.getKey();
            List<GpsRow> storeRows = entry.getValue();

            List<Double> latitudes = storeRows.stream()
                    .map(GpsRow::latitude)
                    .sorted()
                    .toList();

            List<Double> longitudes = storeRows.stream()
                    .map(GpsRow::longitude)
                    .sorted()
                    .toList();

            Double medianLatitude = median(latitudes);
            Double medianLongitude = median(longitudes);
            GpsRow referenceRow = storeRows.get(0);
            String storeName = firstNonBlank(storeRows.stream().map(GpsRow::storeName).toList(), storeCode);
            String storeCity = firstNonBlank(storeRows.stream().map(GpsRow::storeCity).toList(), null);
            String storeState = firstNonBlank(storeRows.stream().map(GpsRow::storeState).toList(), null);
            String storeRegion = firstNonBlank(storeRows.stream().map(GpsRow::storeRegion).toList(), null);
            String storeFormat = firstNonBlank(storeRows.stream().map(GpsRow::storeFormat).toList(), null);

            markers.add(new StoreMapMarkerResponse(
                    storeCode,
                    storeName,
                    storeCity,
                    storeState,
                    storeRegion,
                    storeFormat,
                    medianLatitude,
                    medianLongitude,
                    storeRows.size()
            ));
        }

        markers.sort(Comparator.comparing(
                marker -> marker.storeName() == null ? marker.storeCode() : marker.storeName(),
                String.CASE_INSENSITIVE_ORDER
        ));

        return markers;
    }

    public StoreMapDetailResponse getStoreDetails(String storeCode, Integer year, Integer month) {
        DateFilter dateFilter = buildDateFilter(year, month);

        Integer monthlyVisitCount = jdbcTemplate.query(
                """
                SELECT COUNT(*) AS visit_count
                FROM visits v
                JOIN stores s ON s.store_id = v.store_id
                WHERE s.store_code = ?
                """ + dateFilter.andClause() + "\n",
                rs -> rs.next() ? rs.getInt("visit_count") : 0,
                buildParams(storeCode, dateFilter).toArray()
        );

        LatestVisit latestVisit = jdbcTemplate.query(
                """
                SELECT v.visit_date, v.map_link, e.employee_id, e.username
                FROM visits v
                JOIN stores s ON s.store_id = v.store_id
                JOIN employees e ON e.employee_id = v.employee_id
                WHERE s.store_code = ?
                """ + dateFilter.andClause() + "\n" + """

                ORDER BY v.visit_date DESC, v.visit_id DESC
                LIMIT 1
                """,
                rs -> {
                    if (!rs.next()) {
                        return new LatestVisit(null, null, null, null);
                    }

                    java.sql.Date visitDate = rs.getDate("visit_date");
                    return new LatestVisit(
                            visitDate == null ? null : visitDate.toLocalDate(),
                            rs.getString("username"),
                            rs.getInt("employee_id"),
                            rs.getString("map_link")
                    );
                },
                buildParams(storeCode, dateFilter).toArray()
        );

        OsaStats osaStats = jdbcTemplate.query(
                """
                SELECT
                    COUNT(*) AS total_answers,
                    SUM(CASE WHEN UPPER(TRIM(o.q_est_ce_que_le_sku_ci_dessous_est_disponible)) = 'OUI' THEN 1 ELSE 0 END) AS available_answers
                FROM task_osa_pack_coc_mh o
                JOIN visits v ON v.visit_id = o.visit_id
                JOIN stores s ON s.store_id = v.store_id
                WHERE s.store_code = ?
                """ + dateFilter.andClause() + "\n",
                rs -> {
                    if (!rs.next()) {
                        return new OsaStats(0, 0);
                    }

                    return new OsaStats(
                            rs.getInt("available_answers"),
                            rs.getInt("total_answers")
                    );
                },
                buildParams(storeCode, dateFilter).toArray()
        );

        Integer deviationVisitCount = jdbcTemplate.query(
                """
                SELECT COUNT(DISTINCT v.visit_id) AS deviation_visit_count
                FROM task_callcycle_deviation c
                JOIN visits v ON v.visit_id = c.visit_id
                JOIN stores s ON s.store_id = v.store_id
                WHERE s.store_code = ?
                """ + dateFilter.andClause() + "\n" + """
                  AND UPPER(TRIM(c.q_callcycle_deviation)) = 'OUI'
                """,
                rs -> rs.next() ? rs.getInt("deviation_visit_count") : 0,
                buildParams(storeCode, dateFilter).toArray()
        );

        Double osaPercentage = osaStats.totalAnswers() == 0
                ? null
                : (osaStats.availableAnswers() * 100.0) / osaStats.totalAnswers();

        Double deviationPercentage = monthlyVisitCount == null || monthlyVisitCount == 0
                ? null
                : ((deviationVisitCount == null ? 0 : deviationVisitCount) * 100.0) / monthlyVisitCount;
        Double coverageRatePercentage = deviationPercentage == null ? null : 100.0 - deviationPercentage;

        String coverageStatus;
        if (monthlyVisitCount == null || monthlyVisitCount == 0) {
            coverageStatus = "Not covered";
        } else if (deviationVisitCount != null && deviationVisitCount > 0) {
            coverageStatus = "Deviated";
        } else {
            coverageStatus = "Covered";
        }

        return new StoreMapDetailResponse(
                storeCode,
                osaPercentage,
                coverageStatus,
                coverageRatePercentage,
                deviationPercentage,
                deviationVisitCount,
                monthlyVisitCount,
                latestVisit.lastVisitDate(),
                latestVisit.merchandiserName(),
                latestVisit.merchandiserUserId(),
                latestVisit.mapLink()
        );
    }

    private static List<Object> buildParams(String storeCode, DateFilter dateFilter) {
        List<Object> params = new ArrayList<>();
        params.add(storeCode);
        return params;
    }

    private static DateFilter buildDateFilter(Integer year, Integer month) {
        List<String> conditions = new ArrayList<>();

        if (year != null) {
            conditions.add("YEAR(v.visit_date) = " + year);
        }

        if (month != null) {
            conditions.add("MONTH(v.visit_date) = " + month);
        }

        if (conditions.isEmpty()) {
            return new DateFilter("");
        }

        return new DateFilter(" AND " + String.join(" AND ", conditions));
    }

    private static Double median(List<Double> values) {
        if (values.isEmpty()) {
            return null;
        }

        int size = values.size();
        int middle = size / 2;

        if (size % 2 == 1) {
            return values.get(middle);
        }

        return (values.get(middle - 1) + values.get(middle)) / 2.0;
    }

    private static String firstNonBlank(List<String> values, String fallback) {
        return values.stream()
                .filter(value -> value != null && !value.isBlank())
                .findFirst()
                .orElse(fallback);
    }

    private record GpsRow(
            String storeCode,
            String storeName,
            String storeCity,
            String storeState,
            String storeRegion,
            String storeFormat,
            Double latitude,
            Double longitude
    ) {
    }

    private record DateFilter(String andClause) {
    }

    private record LatestVisit(
            java.time.LocalDate lastVisitDate,
            String merchandiserName,
            Integer merchandiserUserId,
            String mapLink
    ) {
    }

    private record OsaStats(Integer availableAnswers, Integer totalAnswers) {
    }
}
