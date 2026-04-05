package com.smollan.backend.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.smollan.backend.entity.Employee;

public interface EmployeeRepository extends JpaRepository<Employee, Long> {
  Employee findByUsername(String username);
}