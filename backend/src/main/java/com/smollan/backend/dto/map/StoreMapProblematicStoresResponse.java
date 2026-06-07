package com.smollan.backend.dto.map;

import java.time.LocalDate;
import java.util.List;

public record StoreMapProblematicStoresResponse(
        long totalRows,
        ChannelSummary mtSummary,
        ChannelSummary gtSummary,
        List<ProblematicStoreRow> rows
) {
    public record ChannelSummary(
            String channel,
            long issueCount,
            long problematicStoreCount
    ) {
    }

    public record ProblematicStoreRow(
            String employeeCode,
            String merchandiserName,
            String storeCode,
            String storeName,
            String storeFormat,
            String channel,
            LocalDate visitDate,
            Double latitude,
            Double longitude,
            long osaIssueCount,
            long gpsIssueCount,
            long totalIssueCount,
            String mainIssueType
    ) {
    }
}
