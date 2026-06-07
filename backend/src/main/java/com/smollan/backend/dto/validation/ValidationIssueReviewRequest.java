package com.smollan.backend.dto.validation;

public record ValidationIssueReviewRequest(
        String reviewStatus,
        String reviewComment,
        String reviewedBy
) {
}
