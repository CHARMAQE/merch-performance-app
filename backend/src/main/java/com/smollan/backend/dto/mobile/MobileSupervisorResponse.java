package com.smollan.backend.dto.mobile;

public record MobileSupervisorResponse(
        Long supervisorId,
        String supervisorCode,
        String fullName,
        String username,
        String email,
        String city,
        String region,
        String role,
        Long assignedStoreCount
) {
}
