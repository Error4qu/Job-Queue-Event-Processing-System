package com.project.job_queue.model;
import jakarta.persistence.*;

@Entity
public class Job {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String status;

    private String payload;

    public Long getId() { return id; }

    public String getStatus() { return status; }

    public void setStatus(String status) { this.status = status; }

    public String getPayload() { return payload; }

    public void setPayload(String payload) { this.payload = payload; }
}