package com.smollan.backend.controller;

import java.util.List;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.smollan.backend.repository.CallcycleDeviationRepository;
import com.smollan.backend.service.CallcycleDeviationService;

@RestController
@RequestMapping("/api/reports")
@CrossOrigin(origins = "*")
public class CallcycleDeviationController {

    private final CallcycleDeviationService service;

    public CallcycleDeviationController(CallcycleDeviationService service) {
        this.service = service;
    }

    @GetMapping("/deviation-summary")
    public List<CallcycleDeviationRepository.EmployeeDeviationSummary> getDeviationSummaryByEmployee() {
        return service.getDeviationSummaryByEmployee();
    }
}