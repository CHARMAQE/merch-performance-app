package com.smollan.backend.dto.map;

public record StoreMapMarkerResponse(
        String storeCode,
        String storeName,
        String storeCity,
        String storeState,
        String storeRegion,
        String storeFormat,
        Double latitude,
        Double longitude,
        Integer gpsVisitCount
) {
}
