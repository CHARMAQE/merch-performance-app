package com.smollan.backend.dto.mobile;

import java.time.LocalDate;

public record MobileExecutionStoreSummaryResponse(
        String storeCode,
        String storeName,
        String storeCity,
        String storeFormat,
        Long executionCount,
        Long merchandiserCount,
        LocalDate latestVisitDate
) {
}
