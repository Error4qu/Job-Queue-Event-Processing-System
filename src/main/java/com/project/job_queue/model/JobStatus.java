package com.project.job_queue.model;
/** Enum representing all possible lifecycle states of a job. */
public enum JobStatus {
    PENDING,
    SCHEDULED,
    QUEUED,
    PROCESSING,
    COMPLETED,
    FAILED,
    RETRY,
    CANCELLED
}
