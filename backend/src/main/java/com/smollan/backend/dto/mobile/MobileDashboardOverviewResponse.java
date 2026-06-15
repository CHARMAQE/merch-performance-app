package com.smollan.backend.dto.mobile;

import java.time.LocalDate;

public record MobileDashboardOverviewResponse(
        Long plannedVisits,
        Long executedVisits,
        Long deviationVisits,
        Double coverageRate,
        Double deviationRate,
        Double osaPercentage,
        Double sosPercentage,
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
            Long visits,
            LocalDate latestVisitDate,
            Long plannedStores,
            Long coveredStores,
            Long deviationStores
    ) {
    }

    public record DailyReport(
            Long activeMerchandisers
    ) {
    }
}
