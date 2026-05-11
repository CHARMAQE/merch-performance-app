package com.smollan.backend.controller;

import java.util.List;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.smollan.backend.dto.dashboard.DashboardOverviewResponse;
import com.smollan.backend.dto.validation.ValidationIssueResponse;
import com.smollan.backend.service.DashboardService;

@RestController
@RequestMapping("/api/dashboard")
@CrossOrigin(origins = "*")
public class DashboardController {

    private final DashboardService dashboardService;

    public DashboardController(DashboardService dashboardService) {
        this.dashboardService = dashboardService;
    }

    @GetMapping("/overview")
    public DashboardOverviewResponse getOverview(
            @RequestParam(required = false) Integer year,
            @RequestParam(required = false) Integer month,
            @RequestParam(required = false) Integer day,
            @RequestParam(required = false) String storeCode
    ) {
        return dashboardService.getOverview(year, month, day, storeCode);
    }

    @GetMapping("/latest-issues")
    public List<ValidationIssueResponse> getLatestIssues(
            @RequestParam(defaultValue = "12") int limit,
            @RequestParam(required = false) String storeCode
    ) {
        return dashboardService.getLatestIssues(limit, storeCode);
    }
}
