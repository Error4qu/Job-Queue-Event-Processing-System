package com.project.job_queue.model;
import jakarta.persistence.*;
/** JPA entity representing a job in the queue system. */
@Entity
public class Job {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String type;
    @Enumerated(EnumType.STRING)
    private JobStatus status;
    private String payload;
    private String email;
    private String message;
    private int retryCount;
    private Long scheduleTime;
    private Long nextRetryTime;
    @Column(unique = true)
    private String idempotencyKey;
    public Long getId() { return id; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public JobStatus getStatus() { return status; }
    public void setStatus(JobStatus status) { this.status = status; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
    public Long getScheduleTime() { return scheduleTime; }
    public void setScheduleTime(Long scheduleTime) { this.scheduleTime = scheduleTime; }
    public Long getNextRetryTime() { return nextRetryTime; }
    public void setNextRetryTime(Long nextRetryTime) { this.nextRetryTime = nextRetryTime; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
}