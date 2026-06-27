package com.project.job_queue.service;
import com.project.job_queue.model.Job;
import com.project.job_queue.model.JobStatus;
import com.project.job_queue.repository.JobRepository;
import com.project.job_queue.executer.ExecutorFactory;
import com.project.job_queue.executer.JobExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.Map;
/** Kafka consumer that processes jobs with thread-safe circuit breaker, rate limiting and retry logic. */
@Service
public class KafkaConsumerService {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);
    private static final int FAILURE_THRESHOLD = 5;
    private static final long CIRCUIT_TIMEOUT = 30000;
    private static final int MAX_RETRY = 5;
    private final JobRepository repo;
    private final ExecutorFactory executorFactory;
    private final RedisService redisService;
    private final RateLimiterService rateLimiter;
    private final IncidentPublisherService incidentPublisher;
    private final ConcurrentHashMap<String, AtomicInteger> failureCountMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> circuitOpenMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> lastFailureTimeMap = new ConcurrentHashMap<>();
    
    private AtomicInteger getFailureCount(String type) {
        return failureCountMap.computeIfAbsent(type, k -> new AtomicInteger(0));
    }
    
    private boolean isCircuitOpen(String type) {
        return circuitOpenMap.getOrDefault(type, false);
    }
    
    private void setCircuitOpen(String type, boolean value) {
        circuitOpenMap.put(type, value);
    }
    
    private AtomicLong getLastFailureTime(String type) {
        return lastFailureTimeMap.computeIfAbsent(type, k -> new AtomicLong(0));
    }
    /** Constructs the consumer with all required dependencies. */
    public KafkaConsumerService(JobRepository repo, ExecutorFactory executorFactory,
                                RedisService redisService, RateLimiterService rateLimiter,
                                IncidentPublisherService incidentPublisher) {
        this.repo = repo;
        this.executorFactory = executorFactory;
        this.redisService = redisService;
        this.rateLimiter = rateLimiter;
        this.incidentPublisher = incidentPublisher;
    }
    /** Consumes job messages from Kafka, executes them and handles failure scenarios. */
    @KafkaListener(
            topics = "job-topic",
            groupId = "job-group",
            concurrency = "3"
    )
    public void consume(String message, Acknowledgment ack) {
        Long jobId;
        try {
            jobId = Long.parseLong(message);
        } catch (Exception e) {
            log.error("Invalid message received message={}", message);
            ack.acknowledge();
            return;
        }
        Job job = repo.findById(jobId).orElse(null);
        if (job == null) {
            log.warn("Job not found, skipping jobId={}", jobId);
            ack.acknowledge();
            return;
        }
        if (redisService.isCancelled(jobId)) {
            log.info("Job cancelled jobId={}", jobId);
            job.setStatus(JobStatus.CANCELLED);
            repo.save(job);
            redisService.clearCancellation(jobId);
            ack.acknowledge();
            return;
        }
        long now = System.currentTimeMillis();
        if (!rateLimiter.allow(job.getType(), 5)) {
            log.warn("Rate limit hit for type={}, scheduling retry jobId={}", job.getType(), jobId);
            incidentPublisher.publish(
                    "RATE_LIMITED_JOB", "MEDIUM", "Rate limit exceeded",
                    jobId.toString(), job.getType(), "Rate limit hit, job deferred to retry",
                    Map.of("retryDelayMs", "2000")
            );
            job.setStatus(JobStatus.RETRY);
            job.setNextRetryTime(System.currentTimeMillis() + 2000);
            repo.save(job);
            ack.acknowledge();
            return;
        }
        String jobType = job.getType();
        if (isCircuitOpen(jobType)) {
            if (now - getLastFailureTime(jobType).get() < CIRCUIT_TIMEOUT) {
                log.warn("Circuit OPEN for type={}, delaying jobId={}", jobType, jobId);
                incidentPublisher.publish(
                        "CIRCUIT_OPEN", "HIGH", "Circuit breaker open",
                        jobId.toString(), jobType, "Circuit open, job delayed",
                        Map.of("timeoutMs", String.valueOf(CIRCUIT_TIMEOUT))
                );
                job.setStatus(JobStatus.RETRY);
                job.setNextRetryTime(now + 5000);
                repo.save(job);
                ack.acknowledge();
                return;
            }
            log.info("Circuit HALF-OPEN for type={}, testing recovery", jobType);
            setCircuitOpen(jobType, false);
        }
        try {
            if (JobStatus.QUEUED != job.getStatus()) {
                ack.acknowledge();
                return;
            }
            job.setStatus(JobStatus.PROCESSING);
            repo.save(job);
            log.info("Executing jobId={} type={}", jobId, job.getType());
            JobExecutor executor = executorFactory.getExecutor(job.getType());
            executor.execute(job);
            job.setStatus(JobStatus.COMPLETED);
            repo.save(job);
            log.info("Job completed jobId={}", jobId);
            getFailureCount(jobType).set(0);
        } catch (Exception e) {
            log.error("Job failed jobId={} reason={}", jobId, e.getMessage(), e);
            int currentFailures = getFailureCount(jobType).incrementAndGet();
            getLastFailureTime(jobType).set(now);
            if (currentFailures >= FAILURE_THRESHOLD) {
                setCircuitOpen(jobType, true);
                log.error("Circuit OPEN triggered for type={} after {} consecutive failures", jobType, currentFailures);
                incidentPublisher.publish(
                        "CIRCUIT_OPEN", "CRITICAL", "Circuit breaker triggered",
                        jobId.toString(), jobType, e.getMessage(),
                        Map.of("consecutiveFailures", String.valueOf(currentFailures))
                );
            }
            if (job.getRetryCount() >= MAX_RETRY) {
                job.setStatus(JobStatus.FAILED);
                repo.save(job);
                incidentPublisher.publish(
                        "JOB_EXECUTION_FAILED", "HIGH", "Job exhausted retries",
                        jobId.toString(), job.getType(), e.getMessage(),
                        Map.of("retryCount", String.valueOf(job.getRetryCount()))
                );
            } else {
                int retryCount = job.getRetryCount() + 1;
                job.setRetryCount(retryCount);
                long delay = (long) Math.pow(2, retryCount) * 1000;
                delay = Math.min(delay, 20000);
                job.setNextRetryTime(now + delay);
                job.setStatus(JobStatus.RETRY);
                repo.save(job);
            }
        }
        ack.acknowledge();
    }

    /** Resets circuit breaker state for a job type. */
    public void resetCircuit(String jobType) {
        getFailureCount(jobType).set(0);
        setCircuitOpen(jobType, false);
        getLastFailureTime(jobType).set(0);
    }

    /** Returns current circuit breaker states for all tracked job types. */
    public Map<String, Object> getCircuitStates() {
        Map<String, Object> states = new HashMap<>();
        for (String type : failureCountMap.keySet()) {
            Map<String, Object> state = new HashMap<>();
            state.put("open", isCircuitOpen(type));
            state.put("failures", getFailureCount(type).get());
            state.put("lastFailureTime", getLastFailureTime(type).get());
            states.put(type, state);
        }
        return states;
    }
}