package com.smollan.backend.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.smollan.backend.entity.Employee;
import com.smollan.backend.repository.EmployeeRepository;

@Service
public class EmployeeService {

    private final EmployeeRepository repo;

    public EmployeeService(EmployeeRepository repo) {
        this.repo = repo;
    }

    public List<Employee> getAllEmployees() {
        return repo.findAll();
    }

    public Employee getEmployeeByUsername(String username) {
        return repo.findByUsername(username);
    }
}