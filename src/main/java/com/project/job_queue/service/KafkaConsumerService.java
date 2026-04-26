package com.project.job_queue.service;

import com.project.job_queue.model.Job;
import com.project.job_queue.repository.JobRepository;
import com.project.job_queue.executor.ExecutorFactory;
import com.project.job_queue.executor.JobExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
    @Autowired
    private JobRepository repo;
    @Autowired
    private ExecutorFactory executorFactory;
    @Autowired
    private RedisService redisService;
    private int failureCount = 0;
    private boolean circuitOpen = false;
    private long lastFailureTime = 0;
    private static final int FAILURE_THRESHOLD = 5;
    private static final long CIRCUIT_TIMEOUT = 30000; // 30 sec
    private static final int MAX_RETRY = 5;
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
            System.out.println("Invalid message: " + message);
            ack.acknowledge();
            return;
        }
        Job job = repo.findById(jobId).orElse(null);
        if (redisService.isCancelled(jobId)) {
            System.out.println("Job cancelled: " + jobId);

            job.setStatus("CANCELLED");
            repo.save(job);

            ack.acknowledge();
            return;
        }
        if (job == null) {
            System.out.println("Job not found, skipping: " + jobId);
            ack.acknowledge();
            return;
        }
        long now = System.currentTimeMillis();
        if (circuitOpen) {
            if (now - lastFailureTime < CIRCUIT_TIMEOUT) {
                System.out.println("Circuit OPEN → delaying job: " + jobId);
                job.setStatus("RETRY");
                job.setNextRetryTime(now + 5000); // 5 sec delay
                repo.save(job);
                ack.acknowledge();
                return;
            }
            System.out.println("Circuit HALF-OPEN → testing...");
            circuitOpen = false;
        }
        try {
            job.setStatus("PROCESSING");
            repo.save(job);
            System.out.println("➡Executing job: " + jobId);
            JobExecutor executor = executorFactory.getExecutor(job.getType());
            executor.execute(job);
            job.setStatus("COMPLETED");
            repo.save(job);
            System.out.println("Job completed: " + jobId);
            failureCount = 0;
        } catch (Exception e) {
            System.out.println("Job failed: " + jobId + " reason: " + e.getMessage());
            failureCount++;
            lastFailureTime = now;
            if (failureCount >= FAILURE_THRESHOLD) {
                circuitOpen = true;
                System.out.println("Circuit OPEN (too many failures)");
            }
            if (job.getRetryCount() >= MAX_RETRY) {
                job.setStatus("FAILED");
                repo.save(job);
                System.out.println("Max retries reached → FAILED: " + jobId);
            } else {
                int retryCount = job.getRetryCount() + 1;
                job.setRetryCount(retryCount);
                long delay = (long) Math.pow(2, retryCount) * 1000;
                delay = Math.min(delay, 20000); // max 20 sec
                job.setNextRetryTime(now + delay);
                job.setStatus("RETRY");
                repo.save(job);
                System.out.println("Scheduled retry for job " + jobId + " after " + delay + " ms");}
        }
        ack.acknowledge();
    }
}