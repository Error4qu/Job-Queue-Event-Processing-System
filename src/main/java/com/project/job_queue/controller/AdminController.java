package com.project.job_queue.controller;

import com.project.job_queue.dto.AdminActionResult;
import com.project.job_queue.service.AdminService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

/** Admin endpoints used by the self-healing orchestrator for recovery actions. */
@RestController
@RequestMapping("/admin")
public class AdminController {
    private static final Logger log = LoggerFactory.getLogger(AdminController.class);
    private final AdminService adminService;
    private final String apiKey;

    public AdminController(AdminService adminService,
                           @Value("${admin.api-key:change-me-in-production}") String apiKey) {
        this.adminService = adminService;
        this.apiKey = apiKey;
    }

    @PostMapping("/watcher/recover")
    public AdminActionResult recoverWatcher(@RequestHeader("X-Admin-Api-Key") String key) {
        authorize(key);
        return adminService.recoverWatcher();
    }

    @PostMapping("/redis/reconcile")
    public AdminActionResult reconcileRedis(@RequestHeader("X-Admin-Api-Key") String key) {
        authorize(key);
        return adminService.reconcileRedis();
    }

    @PostMapping("/jobs/{id}/requeue")
    public AdminActionResult requeueJob(@RequestHeader("X-Admin-Api-Key") String key,
                                        @PathVariable Long id) {
        authorize(key);
        return adminService.requeueJob(id);
    }

    @PostMapping("/circuit/{type}/reset")
    public AdminActionResult resetCircuit(@RequestHeader("X-Admin-Api-Key") String key,
                                          @PathVariable String type) {
        authorize(key);
        return adminService.resetCircuit(type);
    }

    @GetMapping("/health/detailed")
    public Map<String, Object> detailedHealth(@RequestHeader("X-Admin-Api-Key") String key) {
        authorize(key);
        return adminService.getDetailedHealth();
    }

    private void authorize(String key) {
        if (apiKey == null || apiKey.isBlank() || !apiKey.equals(key)) {
            log.warn("Unauthorized admin API access attempt");
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid admin API key");
        }
    }
}
