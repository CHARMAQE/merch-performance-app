package com.smollan.backend.dto.mobile;

import java.time.LocalDate;

public record MobileMerchandiserStoreResponse(
        String storeCode,
        String storeName,
        String storeCity,
        String storeRegion,
        String storeFormat,
        LocalDate visitDate,
        String executionStatus,
        Boolean isPlanned,
        Boolean isAdhoc,
        String callCycleType,
        String deviationReason,
        Integer taskAssigned,
        Integer taskDone,
        Double taskCompletionRate
) {
}
