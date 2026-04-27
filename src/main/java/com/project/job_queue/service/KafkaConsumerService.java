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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private volatile boolean circuitOpen = false;
    private final AtomicLong lastFailureTime = new AtomicLong(0);
    /** Constructs the consumer with all required dependencies. */
    public KafkaConsumerService(JobRepository repo, ExecutorFactory executorFactory,
                                RedisService redisService, RateLimiterService rateLimiter) {
        this.repo = repo;
        this.executorFactory = executorFactory;
        this.redisService = redisService;
        this.rateLimiter = rateLimiter;
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
            job.setStatus(JobStatus.RETRY);
            job.setNextRetryTime(System.currentTimeMillis() + 2000);
            repo.save(job);
            ack.acknowledge();
            return;
        }
        if (circuitOpen) {
            if (now - lastFailureTime.get() < CIRCUIT_TIMEOUT) {
                log.warn("Circuit OPEN, delaying jobId={}", jobId);
                job.setStatus(JobStatus.RETRY);
                job.setNextRetryTime(now + 5000);
                repo.save(job);
                ack.acknowledge();
                return;
            }
            log.info("Circuit HALF-OPEN, testing recovery");
            circuitOpen = false;
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
            failureCount.set(0);
        } catch (Exception e) {
            log.error("Job failed jobId={} reason={}", jobId, e.getMessage(), e);
            int currentFailures = failureCount.incrementAndGet();
            lastFailureTime.set(now);
            if (currentFailures >= FAILURE_THRESHOLD) {
                circuitOpen = true;
                log.error("Circuit OPEN triggered after {} consecutive failures", currentFailures);
            }
            if (job.getRetryCount() >= MAX_RETRY) {
                job.setStatus(JobStatus.FAILED);
                repo.save(job);
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
}