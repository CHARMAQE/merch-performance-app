package com.smollan.backend.dto.mobile;

import java.time.LocalDate;

public record MobileDashboardOverviewResponse(
        Long plannedVisits,
        Long executedVisits,
        Long nonVisitedVisits,
        Long deviationVisits,
        Long rejectedVisits,
        Long problematicVisits,
        Double coverageRate,
        Double nonVisitedRate,
        Double deviationRate,
        Double rejectionRate,
        Double taskCompletionRate,
        LocalDate latestVisitDate,
        Long activeMerchandisers,
        TableCounts tableCounts,
        StoreActivity storeActivity,
        DailyReport dailyReport
) {
    public record TableCounts(
            Long employees,
            Long stores,
            Long products,
            Long visits,
            Long surveyResponses
    ) {
    }

    public record StoreActivity(
            Long assignedStores,
            Long visitedStores,
            Long notVisitedStores,
            Long visits,
            LocalDate latestVisitDate,
            Long plannedStores,
            Long coveredStores,
            Long deviationStores
    ) {
    }

    public record DailyReport(
            Long activeMerchandisers,
            Long storesRevisited
    ) {
    }
}
