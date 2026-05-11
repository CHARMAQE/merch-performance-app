package com.smollan.backend.dto.mobile;

public record MobileLoginRequest(
        String username,
        String password
) {
}
