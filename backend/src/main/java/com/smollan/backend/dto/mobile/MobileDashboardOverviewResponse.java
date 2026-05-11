package com.smollan.backend.dto.mobile;

import java.time.LocalDate;

public record MobileDashboardOverviewResponse(
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
            LocalDate latestVisitDate
    ) {
    }

    public record DailyReport(
            Long activeMerchandisers,
            Long storesRevisited
    ) {
    }
}
