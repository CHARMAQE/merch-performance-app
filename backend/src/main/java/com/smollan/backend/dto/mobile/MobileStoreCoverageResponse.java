package com.smollan.backend.dto.mobile;

import java.math.BigDecimal;
import java.time.LocalDate;

public record MobileStoreCoverageResponse(
        String storeCode,
        String storeName,
        String city,
        String storeCity,
        String region,
        String storeRegion,
        String storeFormat,
        String employeeCode,
        String username,
        String supervisorName,
        LocalDate visitDate,
        String callStatus,
        String callCycleType,
        Boolean isPlanned,
        Boolean isDone,
        Boolean notVisited,
        Boolean deviation,
        Boolean rejection,
        Integer taskAssigned,
        Integer taskDone,
        Double taskPer,
        String reason,
        BigDecimal masterLatitude,
        BigDecimal masterLongitude,
        Double latitude,
        Double longitude,
        BigDecimal startDistanceMeters,
        BigDecimal endDistanceMeters
) {
}
