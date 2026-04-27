package com.project.job_queue.dto;
import java.time.LocalDateTime;
import java.util.Map;
/** Standard error response DTO returned by the global exception handler. */
public class ErrorResponse {
    private final int status;
    private final String error;
    private final String message;
    private final LocalDateTime timestamp;
    private Map<String, String> fieldErrors;
    public ErrorResponse(int status, String error, String message) {
        this.status = status;
        this.error = error;
        this.message = message;
        this.timestamp = LocalDateTime.now();
    }
    public int getStatus() { return status; }
    public String getError() { return error; }
    public String getMessage() { return message; }
    public LocalDateTime getTimestamp() { return timestamp; }
    public Map<String, String> getFieldErrors() { return fieldErrors; }
    public void setFieldErrors(Map<String, String> fieldErrors) { this.fieldErrors = fieldErrors; }
}
