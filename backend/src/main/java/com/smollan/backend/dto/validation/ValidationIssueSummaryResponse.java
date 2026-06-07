package com.smollan.backend.dto.validation;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public record ValidationIssueSummaryResponse(
        Long validationId,
        Long runId,
        Long visitId,
        LocalDate visitDate,
        String ruleCode,
        String severity,
        String storeCode,
        String storeName,
        String employeeCode,
        String employeeName,
        String productCode,
        String productName,
        String question,
        BigDecimal metricValue,
        String message,
        LocalDateTime detectedAt,
        String reviewStatus,
        String reviewComment,
        String reviewedBy,
        LocalDateTime reviewedAt
) {
}
