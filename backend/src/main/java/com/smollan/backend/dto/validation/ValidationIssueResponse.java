package com.smollan.backend.dto.validation;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public record ValidationIssueResponse(
        Long runId,
        Long visitId,
        LocalDate visitDate,
        String ruleCode,
        String severity,
        String storeCode,
        String employeeCode,
        String productCode,
        String question,
        BigDecimal metricValue,
        String message,
        LocalDateTime detectedAt
) {
}
