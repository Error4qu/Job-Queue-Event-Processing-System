package com.project.job_queue.dto;
public class JobRequest {
    private String payload;
    private String time; // "18:15"
    private String email;
    private String message;
    private String type;
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public String getTime() { return time; }
    public void setTime(String time) { this.time = time; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
}