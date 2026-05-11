package com.smollan.backend.controller;

import java.util.List;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.smollan.backend.dto.map.StoreMapDetailResponse;
import com.smollan.backend.dto.map.StoreMapMarkerResponse;
import com.smollan.backend.service.StoreMapService;

@RestController
@RequestMapping("/api/map")
@CrossOrigin(origins = "*")
public class StoreMapController {

    private final StoreMapService storeMapService;

    public StoreMapController(StoreMapService storeMapService) {
        this.storeMapService = storeMapService;
    }

    @GetMapping("/stores")
    public List<StoreMapMarkerResponse> getStores() {
        return storeMapService.getStoreMarkers();
    }

    @GetMapping("/stores/{storeCode}/details")
    public StoreMapDetailResponse getStoreDetails(
            @PathVariable String storeCode,
            @RequestParam(required = false) Integer year,
            @RequestParam(required = false) Integer month
    ) {
        return storeMapService.getStoreDetails(storeCode, year, month);
    }
}
