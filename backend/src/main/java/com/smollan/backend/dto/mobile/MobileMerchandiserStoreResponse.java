package com.smollan.backend.dto.mobile;

import java.time.LocalDate;

public record MobileMerchandiserStoreResponse(
        Long visitId,
        String storeCode,
        String storeName,
        String storeCity,
        String storeRegion,
        String storeFormat,
        LocalDate visitDate,
        Boolean isPlanned,
        Boolean isAdhoc,
        Boolean hasDeviation,
        String deviationReason
) {
}
