package com.project.job_queue.model;
import jakarta.persistence.*;
@Entity
public class Job {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String payload;
    private String status; // PENDING, QUEUED, PROCESSING, COMPLETED, FAILED
    private int retryCount;
    private long nextRetryTime;
    private long scheduleTime;
    private String type;
    private String email;
    private String message;
    // Getters & Setters
    public Long getId() { return id; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
    public long getNextRetryTime() { return nextRetryTime; }
    public void setNextRetryTime(long nextRetryTime) { this.nextRetryTime = nextRetryTime; }
    public long getScheduleTime() { return scheduleTime; }
    public void setScheduleTime(long scheduleTime) { this.scheduleTime = scheduleTime; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}