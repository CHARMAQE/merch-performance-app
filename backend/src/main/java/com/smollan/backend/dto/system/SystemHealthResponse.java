package com.smollan.backend.dto.system;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record SystemHealthResponse(
        String status,
        String database,
        String timestamp,
        String service,
        String error
) {
}
