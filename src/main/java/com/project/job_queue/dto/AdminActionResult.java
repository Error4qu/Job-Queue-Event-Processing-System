package com.project.job_queue.dto;

import java.time.Instant;
import java.util.List;

/** Result of a remediation action triggered by the orchestrator or an operator. */
public class AdminActionResult {
    private String action;
    private boolean success;
    private String message;
    private int affectedCount;
    private List<String> details;
    private Instant timestamp;

    public AdminActionResult() {}

    public static AdminActionResult ok(String action, String message, int affectedCount, List<String> details) {
        AdminActionResult result = new AdminActionResult();
        result.action = action;
        result.success = true;
        result.message = message;
        result.affectedCount = affectedCount;
        result.details = details;
        result.timestamp = Instant.now();
        return result;
    }

    public static AdminActionResult fail(String action, String message) {
        AdminActionResult result = new AdminActionResult();
        result.action = action;
        result.success = false;
        result.message = message;
        result.affectedCount = 0;
        result.timestamp = Instant.now();
        return result;
    }

    public String getAction() { return action; }
    public boolean isSuccess() { return success; }
    public String getMessage() { return message; }
    public int getAffectedCount() { return affectedCount; }
    public List<String> getDetails() { return details; }
    public Instant getTimestamp() { return timestamp; }
}
