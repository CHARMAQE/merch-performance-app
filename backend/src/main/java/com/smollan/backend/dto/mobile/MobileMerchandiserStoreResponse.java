package com.smollan.backend.dto.mobile;

import java.time.LocalDate;

public record MobileMerchandiserStoreResponse(
        String storeCode,
        String storeName,
        String storeCity,
        String storeFormat,
        LocalDate visitDate,
        String executionStatus,
        String deviationReason
) {
}
