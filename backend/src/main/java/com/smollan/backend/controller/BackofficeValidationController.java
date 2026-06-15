package com.smollan.backend.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.smollan.backend.dto.validation.ValidationIssueDetailResponse;
import com.smollan.backend.dto.validation.ValidationIssueListResponse;
import com.smollan.backend.dto.validation.ValidationIssueReviewRequest;
import com.smollan.backend.service.BackofficeValidationService;

@RestController
@RequestMapping("/api/backoffice/validation/issues")
@CrossOrigin(origins = "*")
public class BackofficeValidationController {

    private final BackofficeValidationService validationService;

    public BackofficeValidationController(BackofficeValidationService validationService) {
        this.validationService = validationService;
    }

    @GetMapping
    public ValidationIssueListResponse listIssues(
            @RequestParam(required = false) String ruleCode,
            @RequestParam(required = false) String severity,
            @RequestParam(required = false) String reviewStatus,
            @RequestParam(required = false) String storeCode,
            @RequestParam(required = false) String employeeCode,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(defaultValue = "0") Integer page,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Integer size
    ) {
        try {
            return validationService.listIssues(
                    ruleCode,
                    severity,
                    reviewStatus,
                    storeCode,
                    employeeCode,
                    startDate,
                    endDate,
                    page,
                    limit == null ? size : limit
            );
        } catch (IllegalArgumentException exc) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, exc.getMessage(), exc);
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<ValidationIssueDetailResponse> getIssue(
            @PathVariable Long id
    ) {
        return validationService.getIssue(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PatchMapping("/{id}/review")
    public ResponseEntity<ValidationIssueDetailResponse> updateReview(
            @PathVariable Long id,
            @RequestBody ValidationIssueReviewRequest request
    ) {
        try {
            return validationService.updateReview(id, request)
                    .map(ResponseEntity::ok)
                    .orElseGet(() -> ResponseEntity.notFound().build());
        } catch (IllegalArgumentException exc) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, exc.getMessage(), exc);
        }
    }
}
