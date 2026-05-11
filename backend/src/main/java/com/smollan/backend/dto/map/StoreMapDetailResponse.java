package com.smollan.backend.dto.map;

import java.time.LocalDate;

public record StoreMapDetailResponse(
        String storeCode,
        Double osaPercentage,
        String coverageStatus,
        Double coverageRatePercentage,
        Double deviationPercentage,
        Integer deviationVisitCount,
        Integer monthlyVisitCount,
        LocalDate lastVisitDate,
        String merchandiserName,
        Integer merchandiserUserId,
        String mapLink
) {
}
