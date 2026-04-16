package com.smollan.backend.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.smollan.backend.repository.CallcycleDeviationRepository;

@Service
public class CallcycleDeviationService {

    private final CallcycleDeviationRepository repo;

    public CallcycleDeviationService(CallcycleDeviationRepository repo) {
        this.repo = repo;
    }

    public List<CallcycleDeviationRepository.EmployeeDeviationSummary> getDeviationSummaryByEmployee() {
        return repo.findDeviationSummaryByEmployee();
    }
}