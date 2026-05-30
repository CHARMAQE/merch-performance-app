package com.smollan.backend.controller;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.smollan.backend.dto.mobile.MobileDashboardOverviewResponse;
import com.smollan.backend.dto.mobile.MobileExecutionStoreSummaryResponse;
import com.smollan.backend.dto.mobile.MobileIssueResponse;
import com.smollan.backend.dto.mobile.MobileLoginRequest;
import com.smollan.backend.dto.mobile.MobileMerchandiserExecutionResponse;
import com.smollan.backend.dto.mobile.MobileMerchandiserStoreResponse;
import com.smollan.backend.dto.mobile.MobileStoreCoverageResponse;
import com.smollan.backend.dto.mobile.MobileStoreDetailResponse;
import com.smollan.backend.dto.mobile.MobileSupervisorResponse;
import com.smollan.backend.service.MobileService;

@RestController
@RequestMapping("/api/mobile")
@CrossOrigin(origins = "*")
public class MobileController {

    private final MobileService mobileService;

    public MobileController(MobileService mobileService) {
        this.mobileService = mobileService;
    }

    @PostMapping("/login")
    public ResponseEntity<MobileSupervisorResponse> login(
            @RequestBody MobileLoginRequest request
    ) {
        return mobileService.login(request)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(401).build());
    }

    @GetMapping("/stores")
    public List<MobileStoreCoverageResponse> getSupervisorStores(
            @RequestParam(required = false) Long supervisorId,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String supervisor,
            @RequestParam(required = false) String merchandiser,
            @RequestParam(required = false) String region,
            @RequestParam(required = false) String storeFormat,
            @RequestParam(required = false) Integer year,
            @RequestParam(required = false) Integer month,
            @RequestParam(required = false) Integer day
    ) {
        return mobileService.getCoverageStores(
                supervisorId, startDate, endDate, supervisor, merchandiser,
                region, storeFormat, year, month, day
        );
    }

    @GetMapping("/overview")
    public MobileDashboardOverviewResponse getSupervisorOverview(
            @RequestParam(required = false) Long supervisorId,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String supervisor,
            @RequestParam(required = false) String merchandiser,
            @RequestParam(required = false) String region,
            @RequestParam(required = false) String storeFormat,
            @RequestParam(required = false) Integer year,
            @RequestParam(required = false) Integer month,
            @RequestParam(required = false) Integer day
    ) {
        return mobileService.getSupervisorOverview(
                supervisorId, startDate, endDate, supervisor, merchandiser,
                region, storeFormat, year, month, day
        );
    }

    @GetMapping("/merchandisers")
    public List<MobileMerchandiserExecutionResponse> getMerchandiserExecution(
            @RequestParam(required = false) Long supervisorId,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String supervisor,
            @RequestParam(required = false) String merchandiser,
            @RequestParam(required = false) String region,
            @RequestParam(required = false) String storeFormat,
            @RequestParam(required = false) String storeFormatGroup,
            @RequestParam(required = false) Integer year,
            @RequestParam(required = false) Integer month,
            @RequestParam(required = false) Integer day
    ) {
        return mobileService.getMerchandiserExecution(
                supervisorId, startDate, endDate, supervisor, merchandiser,
                region, storeFormat, storeFormatGroup, year, month, day
        );
    }

    @GetMapping("/issues")
    public List<MobileIssueResponse> getIssues(
            @RequestParam(required = false) Long supervisorId,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String supervisor,
            @RequestParam(required = false) String merchandiser,
            @RequestParam(required = false) String region,
            @RequestParam(required = false) String storeFormat,
            @RequestParam(required = false) Integer year,
            @RequestParam(required = false) Integer month,
            @RequestParam(required = false) Integer day
    ) {
        return mobileService.getIssues(
                supervisorId, startDate, endDate, supervisor, merchandiser,
                region, storeFormat, year, month, day
        );
    }

    @GetMapping("/merchandisers/{employeeCode}/stores")
    public List<MobileMerchandiserStoreResponse> getMerchandiserStores(
            @PathVariable String employeeCode,
            @RequestParam Long supervisorId,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String storeFormatGroup,
            @RequestParam(required = false) Integer year,
            @RequestParam(required = false) Integer month,
            @RequestParam(required = false) Integer day
    ) {
        return mobileService.getMerchandiserStores(
                supervisorId, employeeCode, startDate, endDate, storeFormatGroup, year, month, day
        );
    }

    @GetMapping("/execution-stores")
    public List<MobileExecutionStoreSummaryResponse> getExecutionStores(
            @RequestParam Long supervisorId,
            @RequestParam String type,
            @RequestParam(required = false) Integer year,
            @RequestParam(required = false) Integer month,
            @RequestParam(required = false) Integer day
    ) {
        return mobileService.getExecutionStores(supervisorId, type, year, month, day);
    }

    @GetMapping("/stores/{storeCode}")
    public ResponseEntity<MobileStoreDetailResponse> getSupervisorStoreDetails(
            @PathVariable String storeCode,
            @RequestParam(required = false) Long supervisorId,
            @RequestParam(required = false) Integer year,
            @RequestParam(required = false) Integer month,
            @RequestParam(required = false) Integer day
    ) {
        return mobileService.getMobileStoreDetails(supervisorId, storeCode, year, month, day)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
