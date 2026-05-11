package com.smollan.backend.dto.dashboard;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public record DashboardOverviewResponse(
        DateCoverage dateCoverage,
        TableCounts tableCounts,
        LatestValidationRun latestValidationRun,
        List<IssueCount> issueCountsByRule,
        List<IssueCount> issueCountsBySeverity
) {
    public record DateCoverage(
            LocalDate minVisitDate,
            LocalDate maxVisitDate,
            Long distinctVisitDates
    ) {
    }

    public record TableCounts(
            Long employees,
            Long stores,
            Long products,
            Long visits,
            Long surveyResponses
    ) {
    }

    public record LatestValidationRun(
            Long runId,
            LocalDateTime startedAt,
            LocalDateTime finishedAt,
            String status,
            Integer rulesExecuted,
            Integer issuesFound
    ) {
    }

    public record IssueCount(
            String label,
            Long issueCount
    ) {
    }
}
