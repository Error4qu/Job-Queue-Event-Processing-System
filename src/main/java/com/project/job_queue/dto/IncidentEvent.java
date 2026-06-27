package com.project.job_queue.dto;

import java.time.Instant;
import java.util.Map;

/** Structured incident event emitted when the system detects an operational issue. */
public class IncidentEvent {
    private String incidentId;
    private String type;
    private String severity;
    private String title;
    private String jobId;
    private String jobType;
    private String evidence;
    private String source;
    private Instant timestamp;
    private Map<String, String> attributes;

    public IncidentEvent() {}

    public String getIncidentId() { return incidentId; }
    public void setIncidentId(String incidentId) { this.incidentId = incidentId; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }
    public String getJobType() { return jobType; }
    public void setJobType(String jobType) { this.jobType = jobType; }
    public String getEvidence() { return evidence; }
    public void setEvidence(String evidence) { this.evidence = evidence; }
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
    public Map<String, String> getAttributes() { return attributes; }
    public void setAttributes(Map<String, String> attributes) { this.attributes = attributes; }
}
