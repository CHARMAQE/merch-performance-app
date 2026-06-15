package com.smollan.backend.dto.mobile;

import java.time.LocalDate;
import java.util.List;

public record MobileMerchandiserExecutionResponse(
        String employeeCode,
        String username,
        String supervisorName,
        String region,
        String city,
        Long plannedVisits,
        Long adhocVisits,
        Long executedVisits,
        Long deviationVisits,
        Double coverageRate,
        Long storesCovered,
        List<String> cities,
        List<String> storeFormats,
        LocalDate latestVisitDate
) {
}
