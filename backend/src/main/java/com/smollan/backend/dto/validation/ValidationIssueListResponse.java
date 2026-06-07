package com.smollan.backend.dto.validation;

import java.util.List;

public record ValidationIssueListResponse(
        Long total,
        Integer page,
        Integer limit,
        List<ValidationIssueSummaryResponse> issues
) {
}
