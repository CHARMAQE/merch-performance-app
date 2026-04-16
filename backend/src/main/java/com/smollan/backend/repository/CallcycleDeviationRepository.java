package com.smollan.backend.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.smollan.backend.entity.Employee;

public interface CallcycleDeviationRepository extends JpaRepository<Employee, Long> {

    @Query(value = """
            SELECT
                e.employee_id AS employeeId,
                e.employee_code AS employeeCode,
                e.username AS username,
                SUM(CASE WHEN t.q_callcycle_deviation = 'Non' THEN 1 ELSE 0 END) AS completedCount,
                SUM(CASE WHEN t.q_callcycle_deviation = 'Oui' THEN 1 ELSE 0 END) AS deviatedCount,
                COUNT(*) AS totalCount
            FROM task_callcycle_deviation t
            JOIN visits v ON v.visit_id = t.visit_id
            JOIN employees e ON e.employee_id = v.employee_id
            GROUP BY e.employee_id, e.employee_code, e.username
            ORDER BY e.username
            """, nativeQuery = true)
    List<EmployeeDeviationSummary> findDeviationSummaryByEmployee();

    interface EmployeeDeviationSummary {
        Long getEmployeeId();
        String getEmployeeCode();
        String getUsername();
        Long getCompletedCount();
        Long getDeviatedCount();
        Long getTotalCount();
    }
}