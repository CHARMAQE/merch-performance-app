package com.smollan.backend.dto.mobile;

public record MobileSupervisorResponse(
        Long supervisorId,
        String fullName,
        String username,
        String phone,
        String email,
        String region,
        String role
) {
}
