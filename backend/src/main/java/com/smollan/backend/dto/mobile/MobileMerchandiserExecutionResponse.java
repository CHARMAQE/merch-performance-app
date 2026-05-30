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
        Double coverageRate,
        Long nonVisitedVisits,
        Long deviationVisits,
        Long rejectedVisits,
        Long problematicVisits,
        Double taskCompletionRate,
        Double avgVisitDuration,
        Double avgDistanceFromStore,
        Long storesCovered,
        List<String> cities,
        List<String> storeFormats,
        LocalDate latestVisitDate
) {
}
