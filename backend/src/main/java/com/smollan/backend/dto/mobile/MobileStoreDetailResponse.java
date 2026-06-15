package com.smollan.backend.dto.mobile;

import java.time.LocalDate;

public record MobileStoreDetailResponse(
        String storeCode,
        String storeName,
        String city,
        String region,
        String storeFormat,
        String username,
        String merchandiserName,
        String supervisorName,
        LocalDate visitDate,
        Long latestVisitId,
        LocalDate latestVisitDate,
        Long monthlyVisitCount,
        Boolean hasDeviation
) {
}
