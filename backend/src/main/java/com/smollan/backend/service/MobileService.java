package com.smollan.backend.service;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.bcrypt.BCrypt;
import org.springframework.stereotype.Service;

import com.smollan.backend.dto.mobile.MobileDashboardOverviewResponse;
import com.smollan.backend.dto.mobile.MobileExecutionStoreSummaryResponse;
import com.smollan.backend.dto.mobile.MobileLoginRequest;
import com.smollan.backend.dto.mobile.MobileMerchandiserExecutionResponse;
import com.smollan.backend.dto.mobile.MobileMerchandiserStoreResponse;
import com.smollan.backend.dto.mobile.MobileStoreCoverageResponse;
import com.smollan.backend.dto.mobile.MobileStoreDetailResponse;
import com.smollan.backend.dto.mobile.MobileSupervisorResponse;
import com.smollan.backend.dto.mobile.MobileTasksOverviewResponse;

@Service
public class MobileService {

    private static final String COVERED_VISIT_CONDITION = "(fc.is_done = 1 AND fc.rejection = 0 AND fc.deviation = 0)";
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm");
    private static final DateTimeFormatter IMAGE_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final Pattern IMAGE_TIMESTAMP_PATTERN = Pattern.compile("(\\d{14})");
    private static final String CHECK_SELFIE_COLUMN = "q_priere_de_prendre_un_selfie_devant_le_magasin";
    private static final String PLANOGRAMME_BEFORE_COLUMN = "q_merci_de_prendre_la_photo_du_planogramme_avant_lex";
    private static final String PLANOGRAMME_AFTER_COLUMN = "q_merci_de_prendre_la_photo_du_planogramme_apres_lex";
    private static final String PLANOGRAMME_SINGLE_COLUMN = "q_merci_de_prendre_une_photo";
    private static final String QUALITY_PHOTO_COLUMN = "q_merci_de_prendre_une_photo";
    private static final String OSA_AVAILABILITY_COLUMN = "q_est_ce_que_le_sku_ci_dessous_est_disponible";

    private final JdbcTemplate jdbcTemplate;

    public MobileService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
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
        Double osaPercentage = getOverviewOsaPercentage(filter);
        Double sosPercentage = getOverviewSosPercentage(filter);

        return jdbcTemplate.query(
                """
                SELECT
                    COALESCE(SUM(fc.is_planned = 1), 0) AS planned_visits,
                    COALESCE(SUM(fc.is_done = 1), 0) AS executed_visits,
                    COALESCE(SUM(fc.deviation = 1), 0) AS deviation_visits,
                    COALESCE(COUNT(DISTINCT fc.employee_code), 0) AS active_merchandisers,
                    COALESCE(COUNT(DISTINCT fc.store_code), 0) AS stores,
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
                    long deviationVisits = rs.getLong("deviation_visits");
                    long stores = rs.getLong("stores");
                    long activeMerchandisers = rs.getLong("active_merchandisers");
                    long visitedStores = executedVisits + deviationVisits;
                    java.sql.Date latestVisitDate = rs.getDate("latest_visit_date");
                    LocalDate latestVisitDateValue = latestVisitDate == null ? null : latestVisitDate.toLocalDate();

                    return new MobileDashboardOverviewResponse(
                            plannedVisits,
                            executedVisits,
                            deviationVisits,
                            rate(visitedStores, plannedVisits),
                            rate(deviationVisits, visitedStores),
                            osaPercentage,
                            sosPercentage,
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
                                    visitedStores,
                                    visitedStores,
                                    latestVisitDateValue,
                                    plannedVisits,
                                    executedVisits,
                                    deviationVisits
                            ),
                            new MobileDashboardOverviewResponse.DailyReport(
                                    activeMerchandisers
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
                    COALESCE(SUM(fc.deviation = 1), 0) AS deviation_visits,
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

        List<Object> params = new ArrayList<>(filter.params());
        params.addAll(filter.params());

        return jdbcTemplate.query(
                """
                SELECT
                    latest.store_code,
                    latest.store_name,
                    latest.store_city,
                    latest.store_region,
                    latest.store_format,
                    latest.username,
                    latest.supervisor_name,
                    latest.visit_date,
                    latest.visit_id AS latest_visit_id,
                    latest.visit_date AS latest_visit_date,
                    counts.monthly_visit_count,
                    latest.deviation AS has_deviation,
                    latest.deviation_reason,
                    latest.latitude,
                    latest.longitude
                FROM (
                    SELECT
                        fc.store_code,
                        fc.store_name,
                        fc.store_city,
                        fc.store_region,
                        fc.store_format,
                        fc.username,
                        COALESCE(fc.l1name, fc.l2name, fc.l3name) AS supervisor_name,
                        fc.visit_date,
                        fc.visit_id,
                        fc.deviation,
                        NULLIF(TRIM(fc.reason), '') AS deviation_reason,
                        fc.master_latitude AS latitude,
                        fc.master_longitude AS longitude,
                        ROW_NUMBER() OVER (
                            PARTITION BY fc.store_code
                            ORDER BY fc.visit_date DESC, fc.visit_id DESC
                        ) AS rn
                    FROM vw_fact_coverage_visit_match fc
                    WHERE fc.visit_id IS NOT NULL
                      AND COALESCE(fc.rejection, 0) = 0
                """ + filter.andClause() + """
                ) latest
                JOIN (
                    SELECT
                        fc.store_code,
                        COUNT(DISTINCT fc.visit_id) AS monthly_visit_count
                    FROM vw_fact_coverage_visit_match fc
                    WHERE fc.visit_id IS NOT NULL
                      AND COALESCE(fc.rejection, 0) = 0
                """ + filter.andClause() + """

                    GROUP BY fc.store_code
                ) counts ON counts.store_code = latest.store_code
                WHERE latest.rn = 1
                ORDER BY latest.visit_date DESC, latest.store_name, latest.store_code
                LIMIT 500
                """,
                (rs, rowNum) -> mapCoverageStore(rs),
                params.toArray()
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
                    fc.visit_id,
                    fc.store_code,
                    fc.store_name,
                    fc.store_city,
                    fc.store_region,
                    fc.store_format,
                    fc.visit_date,
                    fc.is_planned,
                    fc.is_adhoc,
                    fc.deviation,
                    NULLIF(TRIM(fc.reason), '') AS deviation_reason
                FROM vw_fact_coverage_visit_match fc
                WHERE 1 = 1
                """ + filter.andClause() + storeFormatGroupClause + """
                  AND fc.visit_id IS NOT NULL

                ORDER BY fc.visit_date DESC, fc.store_name, fc.store_code
                LIMIT 300
                """,
                (rs, rowNum) -> new MobileMerchandiserStoreResponse(
                        rs.getLong("visit_id"),
                        rs.getString("store_code"),
                        rs.getString("store_name"),
                        rs.getString("store_city"),
                        rs.getString("store_region"),
                        rs.getString("store_format"),
                        toLocalDate(rs, "visit_date"),
                        rs.getBoolean("is_planned"),
                        rs.getBoolean("is_adhoc"),
                        rs.getBoolean("deviation"),
                        rs.getBoolean("deviation") ? rs.getString("deviation_reason") : null
                ),
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
        params.add(storeCode.trim());
        params.addAll(filter.params());

        List<MobileStoreDetailResponse> rows = jdbcTemplate.query(
                """
                SELECT
                    latest.store_code,
                    latest.store_name,
                    latest.store_city,
                    latest.store_region,
                    latest.store_format,
                    latest.username,
                    latest.supervisor_name,
                    latest.visit_date,
                    latest.visit_id AS latest_visit_id,
                    latest.visit_date AS latest_visit_date,
                    counts.monthly_visit_count,
                    latest.deviation AS has_deviation
                FROM (
                    SELECT
                        fc.store_code,
                        fc.store_name,
                        fc.store_city,
                        fc.store_region,
                        fc.store_format,
                        fc.username,
                        COALESCE(fc.l1name, fc.l2name, fc.l3name) AS supervisor_name,
                        fc.visit_date,
                        fc.visit_id,
                        fc.deviation,
                        ROW_NUMBER() OVER (
                            PARTITION BY fc.store_code
                            ORDER BY fc.visit_date DESC, fc.visit_id DESC
                        ) AS rn
                    FROM vw_fact_coverage_visit_match fc
                    WHERE fc.store_code = ?
                      AND fc.visit_id IS NOT NULL
                      AND COALESCE(fc.rejection, 0) = 0
                """ + filter.andClause() + """
                ) latest
                JOIN (
                    SELECT
                        fc.store_code,
                        COUNT(DISTINCT fc.visit_id) AS monthly_visit_count
                    FROM vw_fact_coverage_visit_match fc
                    WHERE fc.store_code = ?
                      AND fc.visit_id IS NOT NULL
                      AND COALESCE(fc.rejection, 0) = 0
                """ + filter.andClause() + """

                    GROUP BY fc.store_code
                ) counts ON counts.store_code = latest.store_code
                WHERE latest.rn = 1
                """,
                (rs, rowNum) -> new MobileStoreDetailResponse(
                        rs.getString("store_code"),
                        rs.getString("store_name"),
                        rs.getString("store_city"),
                        rs.getString("store_region"),
                        rs.getString("store_format"),
                        rs.getString("username"),
                        rs.getString("username"),
                        rs.getString("supervisor_name"),
                        toLocalDate(rs, "visit_date"),
                        getNullableLong(rs, "latest_visit_id"),
                        toLocalDate(rs, "latest_visit_date"),
                        rs.getLong("monthly_visit_count"),
                        rs.getBoolean("has_deviation")
                ),
                params.toArray()
        );

        return rows.stream().findFirst();
    }

    public Optional<MobileTasksOverviewResponse> getVisitTasksOverview(Long supervisorId, Long visitId) {
        if (supervisorId == null || supervisorId <= 0 || visitId == null || visitId <= 0) {
            return Optional.empty();
        }

        if (!canAccessVisit(supervisorId, visitId)) {
            return Optional.empty();
        }

        Optional<VisitTaskHeader> header = getVisitTaskHeader(visitId);
        if (header.isEmpty()) {
            return Optional.empty();
        }

        VisitTaskHeader visit = header.get();
        List<MobileTasksOverviewResponse.ImageInfo> checkInImages = getTaskImages(
                visitId, "task_location_checkin", CHECK_SELFIE_COLUMN, true
        );
        List<MobileTasksOverviewResponse.ImageInfo> checkOutImages = getTaskImages(
                visitId, "task_location_checkout", CHECK_SELFIE_COLUMN, true
        );
        String checkInTime = getFirstImageCaptureTime(checkInImages).orElse(visit.checkInTime());
        String checkOutTime = getFirstImageCaptureTime(checkOutImages).orElse(visit.checkOutTime());
        Optional<SectionSource> planogrammeSource = findPlanogrammeSource(
                visit.storeId(), visit.visitId(), visit.visitDate()
        );
        Optional<SectionSource> osaSource = findOsaSource(
                visit.storeId(), visit.visitId(), visit.visitDate()
        );
        Optional<SectionSource> sosSource = findSosSource(
                visit.storeId(), visit.visitId(), visit.visitDate()
        );
        Optional<SectionSource> qualitySource = findQualitySource(
                visit.storeId(), visit.visitId(), visit.visitDate()
        );
        MobileTasksOverviewResponse.SourceInfo planogrammeSourceInfo = toSourceInfo(
                planogrammeSource, visit.visitId()
        );

        return Optional.of(new MobileTasksOverviewResponse(
                visit.visitId(),
                visit.visitDate(),
                new MobileTasksOverviewResponse.StoreInfo(
                        visit.storeCode(),
                        visit.storeName(),
                        visit.city(),
                        visit.region(),
                        visit.format(),
                        channelFromFormat(visit.format())
                ),
                new MobileTasksOverviewResponse.MerchandiserInfo(
                        visit.employeeCode(),
                        visit.merchandiserName()
                ),
                new MobileTasksOverviewResponse.CheckInOut(
                        checkInTime,
                        checkOutTime,
                        checkInImages,
                        checkOutImages
                ),
                planogrammeSource
                        .map(source -> getVisitPlanogramme(source.visitId()))
                        .orElseGet(List::of),
                planogrammeSourceInfo,
                getVisitOsa(osaSource, visit.visitId()),
                getVisitSos(sosSource, visit.visitId()),
                getVisitQuality(qualitySource, visit.visitId())
        ));
    }

    private Optional<VisitTaskHeader> getVisitTaskHeader(Long visitId) {
        List<VisitTaskHeader> rows = jdbcTemplate.query(
                """
                SELECT
                    v.visit_id,
                    v.visit_date,
                    v.store_id,
                    s.store_code,
                    s.store_name,
                    s.store_city,
                    s.store_region,
                    s.store_format,
                    e.employee_code,
                    e.username AS merchandiser_name,
                    fc.start_time,
                    fc.end_time
                FROM visits v
                JOIN stores s ON s.store_id = v.store_id
                JOIN employees e ON e.employee_id = v.employee_id
                LEFT JOIN vw_fact_coverage_visit_match fc ON fc.visit_id = v.visit_id
                WHERE v.visit_id = ?
                LIMIT 1
                """,
                (rs, rowNum) -> new VisitTaskHeader(
                        rs.getLong("visit_id"),
                        toLocalDate(rs, "visit_date"),
                        rs.getLong("store_id"),
                        rs.getString("store_code"),
                        rs.getString("store_name"),
                        rs.getString("store_city"),
                        rs.getString("store_region"),
                        rs.getString("store_format"),
                        rs.getString("employee_code"),
                        rs.getString("merchandiser_name"),
                        toTimeText(rs, "start_time"),
                        toTimeText(rs, "end_time")
                ),
                visitId
        );

        return rows.stream().findFirst();
    }

    private List<MobileTasksOverviewResponse.ImageInfo> getTaskImages(
            Long visitId,
            String tableName,
            String imageColumn
    ) {
        return getTaskImages(visitId, tableName, imageColumn, false);
    }

    private List<MobileTasksOverviewResponse.ImageInfo> getTaskImages(
            Long visitId,
            String tableName,
            String imageColumn,
            boolean preferImageTimestamp
    ) {
        return jdbcTemplate.query(
                "SELECT NULLIF(TRIM(`" + imageColumn + "`), '') AS image_url, response_date "
                        + "FROM `" + tableName + "` "
                        + "WHERE visit_id = ? "
                        + "AND NULLIF(TRIM(`" + imageColumn + "`), '') IS NOT NULL "
                        + "ORDER BY response_date, id",
                (rs, rowNum) -> {
                    String imageUrl = rs.getString("image_url");
                    LocalDate responseDate = toLocalDate(rs, "response_date");
                    LocalDate preferredDate = preferImageTimestamp
                            ? extractTimestampFromImageUrl(imageUrl)
                                    .map(LocalDateTime::toLocalDate)
                                    .orElse(responseDate)
                            : responseDate;

                    return new MobileTasksOverviewResponse.ImageInfo(
                            imageUrl,
                            preferredDate
                    );
                },
                visitId
        );
    }

    private List<MobileTasksOverviewResponse.PlanogrammeItem> getVisitPlanogramme(Long visitId) {
        return jdbcTemplate.query(
                "SELECT task AS category, "
                        + "NULLIF(TRIM(`" + PLANOGRAMME_BEFORE_COLUMN + "`), '') AS before_image, "
                        + "NULLIF(TRIM(`" + PLANOGRAMME_AFTER_COLUMN + "`), '') AS after_image, "
                        + "NULLIF(TRIM(`" + PLANOGRAMME_SINGLE_COLUMN + "`), '') AS single_image, "
                        + "response_date "
                        + "FROM task_primary_shelf_placement "
                        + "WHERE visit_id = ? "
                        + "AND (NULLIF(TRIM(`" + PLANOGRAMME_BEFORE_COLUMN + "`), '') IS NOT NULL "
                        + "OR NULLIF(TRIM(`" + PLANOGRAMME_AFTER_COLUMN + "`), '') IS NOT NULL "
                        + "OR NULLIF(TRIM(`" + PLANOGRAMME_SINGLE_COLUMN + "`), '') IS NOT NULL) "
                        + "ORDER BY task, response_date, id",
                (rs, rowNum) -> {
                    LocalDate responseDate = toLocalDate(rs, "response_date");
                    String beforeImage = rs.getString("before_image");
                    String afterImage = rs.getString("after_image");
                    String singleImage = rs.getString("single_image");

                    return new MobileTasksOverviewResponse.PlanogrammeItem(
                            rs.getString("category"),
                            isBlank(beforeImage)
                                    ? null
                                    : new MobileTasksOverviewResponse.ImageInfo(beforeImage, responseDate),
                            isBlank(afterImage)
                                    ? null
                                    : new MobileTasksOverviewResponse.ImageInfo(afterImage, responseDate),
                            isBlank(singleImage)
                                    ? null
                                    : new MobileTasksOverviewResponse.ImageInfo(singleImage, responseDate),
                            responseDate
                    );
                },
                visitId
        );
    }

    private MobileTasksOverviewResponse.OsaSummary getVisitOsa(
            Optional<SectionSource> source,
            Long selectedVisitId
    ) {
        MobileTasksOverviewResponse.SourceInfo sourceInfo = toSourceInfo(source, selectedVisitId);
        if (source.isEmpty()) {
            return new MobileTasksOverviewResponse.OsaSummary(
                    sourceInfo,
                    null,
                    0L,
                    0L,
                    List.of()
            );
        }

        Long visitId = source.get().visitId();
        OsaStats total = jdbcTemplate.query(
                "SELECT "
                        + "COALESCE(SUM(CASE WHEN UPPER(TRIM(`" + OSA_AVAILABILITY_COLUMN + "`)) = 'OUI' THEN 1 ELSE 0 END), 0) AS available_answers, "
                        + "COUNT(*) AS total_answers "
                        + "FROM task_osa_pack_coc_mh "
                        + "WHERE visit_id = ?",
                rs -> {
                    if (!rs.next()) {
                        return new OsaStats(0L, 0L);
                    }

                    return new OsaStats(
                            rs.getLong("available_answers"),
                            rs.getLong("total_answers")
                    );
                },
                visitId
        );

        List<MobileTasksOverviewResponse.OsaCategory> categories = jdbcTemplate.query(
                "SELECT "
                        + "COALESCE(NULLIF(TRIM(p.category), ''), 'Uncategorized') AS category, "
                        + "COALESCE(SUM(CASE WHEN UPPER(TRIM(o.`" + OSA_AVAILABILITY_COLUMN + "`)) = 'OUI' THEN 1 ELSE 0 END), 0) AS available_answers, "
                        + "COUNT(*) AS total_answers "
                        + "FROM task_osa_pack_coc_mh o "
                        + "LEFT JOIN products p ON p.product_id = o.product_id "
                        + "WHERE o.visit_id = ? "
                        + "GROUP BY COALESCE(NULLIF(TRIM(p.category), ''), 'Uncategorized') "
                        + "ORDER BY category",
                (rs, rowNum) -> {
                    long availableAnswers = rs.getLong("available_answers");
                    long totalAnswers = rs.getLong("total_answers");
                    return new MobileTasksOverviewResponse.OsaCategory(
                            rs.getString("category"),
                            nullableRate(availableAnswers, totalAnswers),
                            availableAnswers,
                            totalAnswers
                    );
                },
                visitId
        );

        long availableAnswers = total == null ? 0L : total.availableAnswers();
        long totalAnswers = total == null ? 0L : total.totalAnswers();
        return new MobileTasksOverviewResponse.OsaSummary(
                sourceInfo,
                nullableRate(availableAnswers, totalAnswers),
                availableAnswers,
                totalAnswers,
                categories
        );
    }

    private MobileTasksOverviewResponse.SosSummary getVisitSos(
            Optional<SectionSource> source,
            Long selectedVisitId
    ) {
        MobileTasksOverviewResponse.SourceInfo sourceInfo = toSourceInfo(source, selectedVisitId);
        if (source.isEmpty()) {
            return new MobileTasksOverviewResponse.SosSummary(
                    sourceInfo,
                    null,
                    List.of()
            );
        }

        Long visitId = source.get().visitId();
        SosStats total = jdbcTemplate.query(
                """
                SELECT
                    COALESCE(SUM(parsed.sos_value), 0) AS sos_value,
                    COALESCE(SUM(parsed.total_value), 0) AS total_value
                FROM (
                    SELECT
                        CAST(NULLIF(REGEXP_REPLACE(REPLACE(COALESCE(qst_value, ''), ',', '.'), '[^0-9.]', ''), '') AS DECIMAL(12,4)) AS sos_value,
                        CAST(NULLIF(REGEXP_REPLACE(REPLACE(COALESCE(total, ''), ',', '.'), '[^0-9.]', ''), '') AS DECIMAL(12,4)) AS total_value
                    FROM task_sos
                    WHERE visit_id = ?
                ) parsed
                WHERE parsed.total_value > 0
                """,
                rs -> {
                    if (!rs.next()) {
                        return new SosStats(0.0, 0.0);
                    }

                    return new SosStats(
                            getNullableDouble(rs, "sos_value"),
                            getNullableDouble(rs, "total_value")
                    );
                },
                visitId
        );

        List<MobileTasksOverviewResponse.SosCategory> categories = jdbcTemplate.query(
                """
                SELECT
                    parsed.category,
                    COALESCE(SUM(parsed.sos_value), 0) AS sos_value,
                    COALESCE(SUM(parsed.total_value), 0) AS total_value
                FROM (
                    SELECT
                        COALESCE(NULLIF(TRIM(task), ''), 'Uncategorized') AS category,
                        CAST(NULLIF(REGEXP_REPLACE(REPLACE(COALESCE(qst_value, ''), ',', '.'), '[^0-9.]', ''), '') AS DECIMAL(12,4)) AS sos_value,
                        CAST(NULLIF(REGEXP_REPLACE(REPLACE(COALESCE(total, ''), ',', '.'), '[^0-9.]', ''), '') AS DECIMAL(12,4)) AS total_value
                    FROM task_sos
                    WHERE visit_id = ?
                ) parsed
                WHERE parsed.total_value > 0
                GROUP BY parsed.category
                ORDER BY parsed.category
                """,
                (rs, rowNum) -> {
                    Double sosValue = getNullableDouble(rs, "sos_value");
                    Double totalValue = getNullableDouble(rs, "total_value");
                    return new MobileTasksOverviewResponse.SosCategory(
                            rs.getString("category"),
                            nullableRate(sosValue, totalValue),
                            sosValue,
                            totalValue
                    );
                },
                visitId
        );

        Double sosValue = total == null ? null : total.sosValue();
        Double totalValue = total == null ? null : total.totalValue();
        return new MobileTasksOverviewResponse.SosSummary(
                sourceInfo,
                nullableRate(sosValue, totalValue),
                categories
        );
    }

    private MobileTasksOverviewResponse.QualitySummary getVisitQuality(
            Optional<SectionSource> source,
            Long selectedVisitId
    ) {
        MobileTasksOverviewResponse.SourceInfo sourceInfo = toSourceInfo(source, selectedVisitId);
        if (source.isEmpty()) {
            return new MobileTasksOverviewResponse.QualitySummary(
                    sourceInfo,
                    List.of()
            );
        }

        return new MobileTasksOverviewResponse.QualitySummary(
                sourceInfo,
                getTaskImages(source.get().visitId(), "task_quality", QUALITY_PHOTO_COLUMN)
        );
    }

    private Optional<SectionSource> findPlanogrammeSource(
            Long storeId,
            Long selectedVisitId,
            LocalDate selectedVisitDate
    ) {
        return findSectionSourceVisit(
                storeId,
                selectedVisitId,
                selectedVisitDate,
                "SELECT 1 FROM task_primary_shelf_placement p "
                        + "WHERE p.visit_id = v.visit_id "
                        + "AND (NULLIF(TRIM(p.`" + PLANOGRAMME_BEFORE_COLUMN + "`), '') IS NOT NULL "
                        + "OR NULLIF(TRIM(p.`" + PLANOGRAMME_AFTER_COLUMN + "`), '') IS NOT NULL "
                        + "OR NULLIF(TRIM(p.`" + PLANOGRAMME_SINGLE_COLUMN + "`), '') IS NOT NULL)"
        );
    }

    private Optional<SectionSource> findOsaSource(
            Long storeId,
            Long selectedVisitId,
            LocalDate selectedVisitDate
    ) {
        return findSectionSourceVisit(
                storeId,
                selectedVisitId,
                selectedVisitDate,
                "SELECT 1 FROM task_osa_pack_coc_mh o WHERE o.visit_id = v.visit_id"
        );
    }

    private Optional<SectionSource> findSosSource(
            Long storeId,
            Long selectedVisitId,
            LocalDate selectedVisitDate
    ) {
        return findSectionSourceVisit(
                storeId,
                selectedVisitId,
                selectedVisitDate,
                "SELECT 1 FROM task_sos sos WHERE sos.visit_id = v.visit_id"
        );
    }

    private Optional<SectionSource> findQualitySource(
            Long storeId,
            Long selectedVisitId,
            LocalDate selectedVisitDate
    ) {
        return findSectionSourceVisit(
                storeId,
                selectedVisitId,
                selectedVisitDate,
                "SELECT 1 FROM task_quality q "
                        + "WHERE q.visit_id = v.visit_id "
                        + "AND NULLIF(TRIM(q.`" + QUALITY_PHOTO_COLUMN + "`), '') IS NOT NULL"
        );
    }

    private Optional<SectionSource> findSectionSourceVisit(
            Long storeId,
            Long selectedVisitId,
            LocalDate selectedVisitDate,
            String existsClause
    ) {
        if (storeId == null || storeId <= 0 || selectedVisitDate == null) {
            return Optional.empty();
        }

        List<SectionSource> rows = jdbcTemplate.query(
                """
                SELECT v.visit_id, v.visit_date
                FROM visits v
                WHERE v.store_id = ?
                  AND (
                        v.visit_date < ?
                        OR (v.visit_date = ? AND v.visit_id <= ?)
                      )
                  AND EXISTS (
                """ + existsClause + """
                  )
                ORDER BY v.visit_date DESC, v.visit_id DESC
                LIMIT 1
                """,
                (rs, rowNum) -> new SectionSource(
                        rs.getLong("visit_id"),
                        toLocalDate(rs, "visit_date")
                ),
                storeId,
                selectedVisitDate,
                selectedVisitDate,
                selectedVisitId
        );

        return rows.stream().findFirst();
    }

    private static MobileTasksOverviewResponse.SourceInfo toSourceInfo(
            Optional<SectionSource> source,
            Long selectedVisitId
    ) {
        if (source.isEmpty()) {
            return new MobileTasksOverviewResponse.SourceInfo(
                    null,
                    null,
                    false,
                    "No data available"
            );
        }

        SectionSource value = source.get();
        boolean isFallback = selectedVisitId == null || !selectedVisitId.equals(value.visitId());
        return new MobileTasksOverviewResponse.SourceInfo(
                value.visitId(),
                value.visitDate(),
                isFallback,
                isFallback ? "Last available data: " + value.visitDate() : "Current visit data"
        );
    }

    private Double getOverviewOsaPercentage(CoverageFilter filter) {
        OsaStats stats = jdbcTemplate.query(
                """
                SELECT
                    COUNT(*) AS total_answers,
                    COALESCE(SUM(
                        CASE
                            WHEN UPPER(TRIM(o.q_est_ce_que_le_sku_ci_dessous_est_disponible)) = 'OUI'
                            THEN 1
                            ELSE 0
                        END
                    ), 0) AS available_answers
                FROM task_osa_pack_coc_mh o
                JOIN visits v ON v.visit_id = o.visit_id
                JOIN employees e ON e.employee_id = v.employee_id
                JOIN stores s ON s.store_id = v.store_id
                JOIN fact_coverage fc
                  ON fc.visit_date = v.visit_date
                 AND fc.employee_code = e.employee_code
                 AND fc.store_code = s.store_code
                WHERE 1 = 1
                """ + filter.andClause(),
                rs -> {
                    if (!rs.next()) {
                        return new OsaStats(0L, 0L);
                    }

                    return new OsaStats(
                            rs.getLong("available_answers"),
                            rs.getLong("total_answers")
                    );
                },
                filter.params().toArray()
        );

        if (stats == null || stats.totalAnswers() <= 0) {
            return null;
        }

        return rate(stats.availableAnswers(), stats.totalAnswers());
    }

    private Double getOverviewSosPercentage(CoverageFilter filter) {
        SosStats stats = jdbcTemplate.query(
                """
                SELECT
                    COALESCE(SUM(parsed.sos_value), 0) AS sos_value,
                    COALESCE(SUM(parsed.total_value), 0) AS total_value
                FROM (
                    SELECT
                        CAST(NULLIF(REGEXP_REPLACE(REPLACE(COALESCE(sos.qst_value, ''), ',', '.'), '[^0-9.]', ''), '') AS DECIMAL(12,4)) AS sos_value,
                        CAST(NULLIF(REGEXP_REPLACE(REPLACE(COALESCE(sos.total, ''), ',', '.'), '[^0-9.]', ''), '') AS DECIMAL(12,4)) AS total_value
                    FROM task_sos sos
                    JOIN visits v ON v.visit_id = sos.visit_id
                    JOIN employees e ON e.employee_id = v.employee_id
                    JOIN stores s ON s.store_id = v.store_id
                    JOIN fact_coverage fc
                      ON fc.visit_date = v.visit_date
                     AND fc.employee_code = e.employee_code
                     AND fc.store_code = s.store_code
                    WHERE 1 = 1
                """ + filter.andClause() + """
                ) parsed
                WHERE parsed.total_value > 0
                """,
                rs -> {
                    if (!rs.next()) {
                        return new SosStats(0.0, 0.0);
                    }

                    return new SosStats(
                            getNullableDouble(rs, "sos_value"),
                            getNullableDouble(rs, "total_value")
                    );
                },
                filter.params().toArray()
        );

        if (stats == null || stats.totalValue() == null || stats.totalValue() <= 0) {
            return null;
        }

        double sosValue = stats.sosValue() == null ? 0.0 : stats.sosValue();
        return (sosValue * 100.0) / stats.totalValue();
    }

    private MobileMerchandiserExecutionResponse mapMerchandiserExecution(ResultSet rs) throws SQLException {
        long plannedVisits = rs.getLong("planned_visits");
        long executedVisits = rs.getLong("executed_visits");

        return new MobileMerchandiserExecutionResponse(
                rs.getString("employee_code"),
                rs.getString("username"),
                rs.getString("supervisor_name"),
                rs.getString("region"),
                rs.getString("city"),
                plannedVisits,
                rs.getLong("adhoc_visits"),
                executedVisits,
                rs.getLong("deviation_visits"),
                rate(executedVisits, plannedVisits),
                rs.getLong("stores_covered"),
                splitCommaSeparatedValues(rs.getString("cities")),
                splitCommaSeparatedValues(rs.getString("store_formats")),
                toLocalDate(rs, "latest_visit_date")
        );
    }

    private MobileStoreCoverageResponse mapCoverageStore(ResultSet rs) throws SQLException {
        return new MobileStoreCoverageResponse(
                rs.getString("store_code"),
                rs.getString("store_name"),
                rs.getString("store_city"),
                rs.getString("store_city"),
                rs.getString("store_region"),
                rs.getString("store_region"),
                rs.getString("store_format"),
                rs.getString("username"),
                rs.getString("username"),
                rs.getString("supervisor_name"),
                toLocalDate(rs, "visit_date"),
                getNullableLong(rs, "latest_visit_id"),
                toLocalDate(rs, "latest_visit_date"),
                rs.getLong("monthly_visit_count"),
                rs.getBoolean("has_deviation"),
                rs.getBoolean("has_deviation") ? rs.getString("deviation_reason") : null,
                getNullableDouble(rs, "latitude"),
                getNullableDouble(rs, "longitude")
        );
    }

    private MobileDashboardOverviewResponse emptyOverview() {
        return new MobileDashboardOverviewResponse(
                0L,
                0L,
                0L,
                0.0,
                0.0,
                null,
                null,
                null,
                0L,
                new MobileDashboardOverviewResponse.TableCounts(0L, 0L, 0L, 0L, 0L),
                new MobileDashboardOverviewResponse.StoreActivity(0L, 0L, 0L, null, 0L, 0L, 0L),
                new MobileDashboardOverviewResponse.DailyReport(0L)
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

    private boolean canAccessVisit(Long supervisorId, Long visitId) {
        if (supervisorId == null || supervisorId <= 0 || visitId == null || visitId <= 0) {
            return false;
        }

        if (isAdmin(supervisorId)) {
            return visitExists(visitId);
        }

        Integer count = jdbcTemplate.query(
                """
                SELECT COUNT(*) AS assignment_count
                FROM visits v
                JOIN stores s ON s.store_id = v.store_id
                JOIN supervisor_stores ss ON ss.store_id = s.store_id
                JOIN supervisors sup ON sup.supervisor_id = ss.supervisor_id
                WHERE v.visit_id = ?
                  AND ss.supervisor_id = ?
                  AND ss.active = TRUE
                  AND sup.active = TRUE
                """,
                rs -> rs.next() ? rs.getInt("assignment_count") : 0,
                visitId,
                supervisorId
        );

        return count != null && count > 0;
    }

    private boolean visitExists(Long visitId) {
        Integer count = jdbcTemplate.query(
                """
                SELECT COUNT(*) AS visit_count
                FROM visits
                WHERE visit_id = ?
                """,
                rs -> rs.next() ? rs.getInt("visit_count") : 0,
                visitId
        );

        return count != null && count > 0;
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

    private static Double nullableRate(long numerator, long denominator) {
        if (denominator <= 0) {
            return null;
        }

        return (numerator * 100.0) / denominator;
    }

    private static Double nullableRate(Double numerator, Double denominator) {
        if (denominator == null || denominator <= 0) {
            return null;
        }

        double safeNumerator = numerator == null ? 0.0 : numerator;
        return (safeNumerator * 100.0) / denominator;
    }

    private static String channelFromFormat(String format) {
        if (isBlank(format)) {
            return null;
        }

        return "GROCERY".equalsIgnoreCase(format.trim()) ? "GT" : "MT";
    }

    private static LocalDate toLocalDate(ResultSet rs, String column) throws SQLException {
        java.sql.Date value = rs.getDate(column);
        return value == null ? null : value.toLocalDate();
    }

    private static String toTimeText(ResultSet rs, String column) throws SQLException {
        java.sql.Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toLocalDateTime().toLocalTime().format(TIME_FORMATTER);
    }

    private static Optional<String> getFirstImageCaptureTime(
            List<MobileTasksOverviewResponse.ImageInfo> images
    ) {
        if (images == null || images.isEmpty()) {
            return Optional.empty();
        }

        return images.stream()
                .map(MobileTasksOverviewResponse.ImageInfo::url)
                .map(MobileService::extractTimestampFromImageUrl)
                .flatMap(Optional::stream)
                .findFirst()
                .map(value -> value.toLocalTime().format(TIME_FORMATTER));
    }

    private static Optional<LocalDateTime> extractTimestampFromImageUrl(String url) {
        String fileName = extractUrlParameter(url, "FileName");
        if (isBlank(fileName)) {
            return Optional.empty();
        }

        Matcher matcher = IMAGE_TIMESTAMP_PATTERN.matcher(fileName);
        while (matcher.find()) {
            try {
                return Optional.of(LocalDateTime.parse(matcher.group(1), IMAGE_TIMESTAMP_FORMATTER));
            } catch (DateTimeParseException ignored) {
                // Try the next timestamp-like token if the current one is malformed.
            }
        }

        return Optional.empty();
    }

    private static String extractUrlParameter(String url, String parameterName) {
        if (isBlank(url) || isBlank(parameterName)) {
            return null;
        }

        String normalizedUrl = url.replace("&amp;", "&");
        int queryStart = normalizedUrl.indexOf('?');
        String query = queryStart >= 0 ? normalizedUrl.substring(queryStart + 1) : normalizedUrl;

        for (String part : query.split("&")) {
            int separatorIndex = part.indexOf('=');
            String rawKey = separatorIndex >= 0 ? part.substring(0, separatorIndex) : part;
            String key = decodeUrlValue(rawKey);

            if (parameterName.equalsIgnoreCase(key)) {
                String rawValue = separatorIndex >= 0 ? part.substring(separatorIndex + 1) : "";
                return decodeUrlValue(rawValue);
            }
        }

        return null;
    }

    private static String decodeUrlValue(String value) {
        if (value == null) {
            return null;
        }

        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException error) {
            return value;
        }
    }

    private static Double getNullableDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }

    private static Long getNullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
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

    private record OsaStats(Long availableAnswers, Long totalAnswers) {
    }

    private record SosStats(Double sosValue, Double totalValue) {
    }

    private record VisitTaskHeader(
            Long visitId,
            LocalDate visitDate,
            Long storeId,
            String storeCode,
            String storeName,
            String city,
            String region,
            String format,
            String employeeCode,
            String merchandiserName,
            String checkInTime,
            String checkOutTime
    ) {
    }

    private record SectionSource(
            Long visitId,
            LocalDate visitDate
    ) {
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
