package com.smollan.backend.controller;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.smollan.backend.dto.map.StoreMapProblematicStoresResponse;
import com.smollan.backend.service.BackofficeStoreMapService;

@RestController
@RequestMapping("/api/backoffice/store-map")
@CrossOrigin(origins = "*")
public class BackofficeStoreMapController {

    private final BackofficeStoreMapService storeMapService;

    public BackofficeStoreMapController(BackofficeStoreMapService storeMapService) {
        this.storeMapService = storeMapService;
    }

    @GetMapping("/problematic-stores")
    public StoreMapProblematicStoresResponse getProblematicStores(
            @RequestParam(defaultValue = "ALL") String channel,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String storeCode,
            @RequestParam(required = false) String employeeCode,
            @RequestParam(defaultValue = "100") Integer limit
    ) {
        try {
            return storeMapService.getProblematicStores(
                    channel,
                    startDate,
                    endDate,
                    storeCode,
                    employeeCode,
                    limit
            );
        } catch (IllegalArgumentException exc) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, exc.getMessage(), exc);
        }
    }
}
