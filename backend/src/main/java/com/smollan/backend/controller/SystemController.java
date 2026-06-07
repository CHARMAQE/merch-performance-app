package com.smollan.backend.controller;

import java.time.Instant;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.smollan.backend.dto.system.SystemHealthResponse;

@RestController
@RequestMapping("/api/system")
@CrossOrigin(origins = "*")
public class SystemController {

    private static final String SERVICE_NAME = "merch-performance-backend";

    private final JdbcTemplate jdbcTemplate;

    public SystemController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/health")
    public ResponseEntity<SystemHealthResponse> getHealth() {
        String timestamp = Instant.now().toString();

        try {
            jdbcTemplate.queryForObject("SELECT 1", Integer.class);

            return ResponseEntity.ok(new SystemHealthResponse(
                    "UP",
                    "CONNECTED",
                    timestamp,
                    SERVICE_NAME,
                    null
            ));
        } catch (Exception exc) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(new SystemHealthResponse(
                            "DOWN",
                            "DISCONNECTED",
                            timestamp,
                            SERVICE_NAME,
                            shortErrorMessage(exc)
                    ));
        }
    }

    private static String shortErrorMessage(Exception exc) {
        String message = exc.getMessage();
        if (message == null || message.isBlank()) {
            return exc.getClass().getSimpleName();
        }

        String firstLine = message.lines()
                .findFirst()
                .orElse(message)
                .trim();

        return firstLine.length() <= 160 ? firstLine : firstLine.substring(0, 160);
    }
}
