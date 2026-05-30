package com.smollan.backend.dto.mobile;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public record MobileStoreDetailResponse(
        String storeCode,
        String storeName,
        String city,
        String region,
        String storeFormat,
        String employeeCode,
        String username,
        String supervisorName,
        LocalDate visitDate,
        LocalDate latestVisitDate,
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
        String userAttendance,
        String superiorAttendance,
        String finalUserAttendance,
        BigDecimal masterLatitude,
        BigDecimal masterLongitude,
        BigDecimal startDistanceMeters,
        BigDecimal endDistanceMeters,
        LocalDateTime startTime,
        LocalDateTime endTime
) {
}
