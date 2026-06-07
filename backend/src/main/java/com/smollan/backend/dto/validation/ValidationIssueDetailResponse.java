package com.smollan.backend.dto.validation;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public record ValidationIssueDetailResponse(
        Long validationId,
        Long runId,
        String ruleCode,
        String entityType,
        String entityId,
        Long visitId,
        LocalDate visitDate,
        String storeCode,
        String storeName,
        String employeeCode,
        String employeeName,
        String productCode,
        String productName,
        String question,
        String actualValue,
        String expectedValue,
        BigDecimal metricValue,
        String message,
        String severity,
        String detailsJson,
        LocalDateTime detectedAt,
        String reviewStatus,
        String reviewComment,
        String reviewedBy,
        LocalDateTime reviewedAt
) {
}
