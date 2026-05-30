package com.smollan.backend.dto.mobile;

import java.time.LocalDate;

public record MobileIssueResponse(
        String type,
        String storeCode,
        String storeName,
        String username,
        String supervisorName,
        String city,
        String region,
        String reason,
        LocalDate visitDate,
        String severity
) {
}
