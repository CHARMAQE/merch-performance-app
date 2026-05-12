package com.smollan.backend.dto.mobile;

import java.time.LocalDate;
import java.util.List;

public record MobileMerchandiserExecutionResponse(
        String employeeCode,
        String username,
        Long plannedVisits,
        Long deviationVisits,
        Long storesCovered,
        List<String> cities,
        LocalDate latestVisitDate
) {
}
