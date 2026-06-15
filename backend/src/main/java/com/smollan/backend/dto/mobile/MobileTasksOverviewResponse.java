package com.smollan.backend.dto.mobile;

import java.time.LocalDate;
import java.util.List;

public record MobileTasksOverviewResponse(
        Long visitId,
        LocalDate visitDate,
        StoreInfo store,
        MerchandiserInfo merchandiser,
        CheckInOut checkInOut,
        List<PlanogrammeItem> planogramme,
        SourceInfo planogrammeSource,
        OsaSummary osa,
        SosSummary sos,
        QualitySummary quality
) {
    public record SourceInfo(
            Long sourceVisitId,
            LocalDate sourceVisitDate,
            Boolean isFallback,
            String message
    ) {
    }

    public record StoreInfo(
            String storeCode,
            String storeName,
            String city,
            String region,
            String format,
            String channel
    ) {
    }

    public record MerchandiserInfo(
            String employeeCode,
            String name
    ) {
    }

    public record CheckInOut(
            String checkInTime,
            String checkOutTime,
            List<ImageInfo> checkInImages,
            List<ImageInfo> checkOutImages
    ) {
    }

    public record ImageInfo(
            String url,
            LocalDate responseDate
    ) {
    }

    public record PlanogrammeItem(
            String category,
            ImageInfo beforeImage,
            ImageInfo afterImage,
            ImageInfo singleImage,
            LocalDate responseDate
    ) {
    }

    public record OsaSummary(
            SourceInfo source,
            Double percentage,
            Long availableCount,
            Long totalCount,
            List<OsaCategory> categories
    ) {
    }

    public record OsaCategory(
            String category,
            Double percentage,
            Long availableCount,
            Long totalCount
    ) {
    }

    public record SosSummary(
            SourceInfo source,
            Double percentage,
            List<SosCategory> categories
    ) {
    }

    public record SosCategory(
            String category,
            Double percentage,
            Double value,
            Double total
    ) {
    }

    public record QualitySummary(
            SourceInfo source,
            List<ImageInfo> images
    ) {
    }
}
