package com.smollan.backend.dto.mobile;

public record MobileLoginRequest(
        String email,
        String password
) {
}
