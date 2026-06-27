package com.project.job_queue.service;

import com.project.job_queue.dto.AdminActionResult;
import com.project.job_queue.model.Job;
import com.project.job_queue.model.JobStatus;
import com.project.job_queue.repository.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Admin recovery operations (watcher recovery, Redis reconcile, requeue, circuit reset). */
@Service
public class AdminService {
    private static final Logger log = LoggerFactory.getLogger(AdminService.class);

    private final WatcherService watcherService;
    private final JobRepository repo;
    private final RedisDelayQueueService redisQueue;
    private final KafkaProducerService producer;
    private final KafkaConsumerService consumerService;

    public AdminService(WatcherService watcherService, JobRepository repo,
                        RedisDelayQueueService redisQueue, KafkaProducerService producer,
                        KafkaConsumerService consumerService) {
        this.watcherService = watcherService;
        this.repo = repo;
        this.redisQueue = redisQueue;
        this.producer = producer;
        this.consumerService = consumerService;
    }

    /** Triggers watcher gap recovery for missed schedule windows. */
    public AdminActionResult recoverWatcher() {
        log.info("Admin triggered watcher recovery");
        int recovered = watcherService.triggerRecovery();
        return AdminActionResult.ok(
                "RECOVER_MISSED_SCHEDULE_WINDOWS",
                "Watcher recovery completed",
                recovered,
                List.of("Recovered jobs reinserted into Redis delay queue")
        );
    }

    /** Reconciles Redis delay queue with MySQL source of truth. */
    public AdminActionResult reconcileRedis() {
        log.info("Admin triggered Redis reconciliation");
        long now = System.currentTimeMillis();
        List<Job> overdue = repo.findOverdueScheduled(now, PageRequest.of(0, 500));
        List<String> details = new ArrayList<>();
        int count = 0;

        for (Job job : overdue) {
            if (JobStatus.SCHEDULED != job.getStatus()) {
                continue;
            }
            redisQueue.scheduleJob(job.getId(), job.getScheduleTime());
            details.add("Reinserted jobId=" + job.getId());
            count++;
        }

        List<Job> retryDue = repo.findRetryDue(now, PageRequest.of(0, 500));
        for (Job job : retryDue) {
            if (JobStatus.RETRY != job.getStatus()) {
                continue;
            }
            job.setStatus(JobStatus.SCHEDULED);
            repo.save(job);
            redisQueue.scheduleJob(job.getId(), job.getNextRetryTime());
            details.add("Requeued retry jobId=" + job.getId());
            count++;
        }

        return AdminActionResult.ok(
                "RECONCILE_REDIS_DELAY_QUEUE",
                "Redis reconciliation completed",
                count,
                details
        );
    }

    /** Requeues a single job to Kafka for re-execution. */
    public AdminActionResult requeueJob(Long jobId) {
        Job job = repo.findById(jobId).orElse(null);
        if (job == null) {
            return AdminActionResult.fail("REQUEUE_JOB", "Job not found: " + jobId);
        }
        if (job.getStatus() == JobStatus.COMPLETED || job.getStatus() == JobStatus.CANCELLED) {
            return AdminActionResult.fail("REQUEUE_JOB",
                    "Cannot requeue terminal job in status " + job.getStatus());
        }

        job.setStatus(JobStatus.QUEUED);
        repo.save(job);
        producer.sendJob(jobId.toString());
        log.info("Admin requeued jobId={}", jobId);
        return AdminActionResult.ok(
                "REQUEUE_JOB",
                "Job requeued to Kafka",
                1,
                List.of("jobId=" + jobId + " status=QUEUED")
        );
    }

    /** Resets the circuit breaker for a job type. */
    public AdminActionResult resetCircuit(String jobType) {
        consumerService.resetCircuit(jobType);
        log.info("Admin reset circuit for type={}", jobType);
        return AdminActionResult.ok(
                "RESET_CIRCUIT",
                "Circuit breaker reset for " + jobType,
                1,
                List.of("type=" + jobType)
        );
    }

    /** Returns detailed system health for agent context gathering. */
    public Map<String, Object> getDetailedHealth() {
        Map<String, Object> health = new LinkedHashMap<>();
        Map<String, Long> stats = new LinkedHashMap<>();
        for (JobStatus status : JobStatus.values()) {
            stats.put(status.name(), repo.countByStatus(status));
        }
        health.put("jobStats", stats);
        health.put("circuitStates", consumerService.getCircuitStates());
        health.put("overdueScheduled", repo.findOverdueScheduled(
                System.currentTimeMillis(), PageRequest.of(0, 10)).size());
        return health;
    }
}
