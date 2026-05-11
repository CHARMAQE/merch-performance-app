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

import com.smollan.backend.dto.map.StoreMapDetailResponse;
import com.smollan.backend.dto.map.StoreMapMarkerResponse;
import com.smollan.backend.dto.mobile.MobileDashboardOverviewResponse;
import com.smollan.backend.dto.mobile.MobileLoginRequest;
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
    public List<StoreMapMarkerResponse> getSupervisorStores(
            @RequestParam Long supervisorId
    ) {
        return mobileService.getSupervisorStores(supervisorId);
    }

    @GetMapping("/overview")
    public MobileDashboardOverviewResponse getSupervisorOverview(
            @RequestParam Long supervisorId,
            @RequestParam(required = false) Integer year,
            @RequestParam(required = false) Integer month,
            @RequestParam(required = false) Integer day
    ) {
        return mobileService.getSupervisorOverview(supervisorId, year, month, day);
    }

    @GetMapping("/stores/{storeCode}")
    public ResponseEntity<StoreMapDetailResponse> getSupervisorStoreDetails(
            @PathVariable String storeCode,
            @RequestParam Long supervisorId
    ) {
        return mobileService.getSupervisorStoreDetails(supervisorId, storeCode)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
