package com.smollan.backend.dto.mobile;

import java.time.LocalDate;

public record MobileStoreCoverageResponse(
        String storeCode,
        String storeName,
        String city,
        String storeCity,
        String region,
        String storeRegion,
        String storeFormat,
        String username,
        String merchandiserName,
        String supervisorName,
        LocalDate visitDate,
        Long latestVisitId,
        LocalDate latestVisitDate,
        Long monthlyVisitCount,
        Boolean hasDeviation,
        String deviationReason,
        Double latitude,
        Double longitude
) {
}
