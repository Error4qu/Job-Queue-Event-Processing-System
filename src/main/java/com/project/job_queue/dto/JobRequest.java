package com.project.job_queue.dto;
/** Request DTO for job creation containing scheduling and execution details. */
public class JobRequest {
    private String payload;
    private String time;
    private String email;
    private String message;
    private String type;
    public String getPayload() { return payload; }
    public String getTime() { return time; }
    public String getMessage() { return message; }
    public String getEmail() { return email; }
    public String getType() { return type; }
}