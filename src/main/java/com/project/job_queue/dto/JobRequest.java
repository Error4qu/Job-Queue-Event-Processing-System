package com.project.job_queue.dto;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
/** Request DTO for job creation containing scheduling and execution details. */
public class JobRequest {
    private String payload;
    @Pattern(regexp = "^([01]\\d|2[0-3]):[0-5]\\d$", message = "Time must be in HH:mm format")
    private String time;
    private String email;
    private String message;
    @NotBlank(message = "Job type is required")
    @Pattern(regexp = "^(EMAIL|API|LOG)$", message = "Type must be one of: EMAIL, API, LOG")
    private String type;
    public String getPayload() { return payload; }
    public String getTime() { return time; }
    public String getMessage() { return message; }
    public String getEmail() { return email; }
    public String getType() { return type; }
}