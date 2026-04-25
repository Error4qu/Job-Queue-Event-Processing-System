package com.project.job_queue.service;
import com.project.job_queue.model.Job;
import com.project.job_queue.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
@Service
public class KafkaConsumerService {
    @Autowired
    private JobRepository repo;
    @Autowired
    private KafkaProducerService producer;
    // Circuit breaker state
    private int failureCount = 0;
    private boolean circuitOpen = false;
    private long lastFailureTime = 0;
    private static final int FAILURE_THRESHOLD = 5;
    private static final long CIRCUIT_TIMEOUT = 30000; // 30 sec
    @KafkaListener(
            topics = "job-topic",
            groupId = "job-group",
            concurrency = "3"
    )
    public void consume(String message, Acknowledgment ack) {
        Long jobId = Long.parseLong(message);
        Job job = repo.findById(jobId).orElseThrow();
        long now = System.currentTimeMillis();
        // CIRCUIT BREAKER CHECK
        if (circuitOpen) {
            if (now - lastFailureTime < CIRCUIT_TIMEOUT) {
                System.out.println("Circuit OPEN → sending to retry " + jobId);
                producer.sendRetry(message);
                // ACK original → prevent duplicate
                ack.acknowledge();
                return;
            }
            System.out.println("Circuit HALF-OPEN → testing...");
            circuitOpen = false;
        }
        // 🟢 PROCESS JOB
        try {
            job.setStatus("PROCESSING");
            repo.save(job);
            Thread.sleep(2000); // simulate work
            // simulate random failure
            if (Math.random() < 0.5) {
                throw new RuntimeException("Random failure");
            }
            job.setStatus("COMPLETED");
            repo.save(job);
            System.out.println("Job " + jobId + " completed");
            // reset circuit
            failureCount = 0;
            // ACK success
            ack.acknowledge();
        } catch (Exception e) {
            System.out.println("Job " + jobId + " failed");
            failureCount++;
            lastFailureTime = now;
            //OPEN CIRCUIT
            if (failureCount >= FAILURE_THRESHOLD) {
                circuitOpen = true;
                System.out.println("Circuit OPEN → too many failures");
            }
            //RETRY LOGIC
            if (job.getRetryCount() < 3) {
                int retryCount = job.getRetryCount() + 1;
                job.setRetryCount(retryCount);
                //exponential backoff
                long delay = (long) Math.pow(2, retryCount) * 1000;
                delay = Math.min(delay, 20000);
                job.setNextRetryTime(now + delay);
                job.setStatus("PENDING");
                repo.save(job);
                System.out.println("Retrying job " + jobId + " after " + delay + " ms");
                producer.sendRetry(message);
            } else {
                job.setStatus("FAILED");
                repo.save(job);
                System.out.println("Job " + jobId + " moved to dead topic");
                producer.sendDead(message);
            }
            ack.acknowledge();
        }
    }
}