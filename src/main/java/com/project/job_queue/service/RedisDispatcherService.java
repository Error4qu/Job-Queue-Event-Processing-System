package com.project.job_queue.service;
import com.project.job_queue.model.Job;
import com.project.job_queue.model.JobStatus;
import com.project.job_queue.repository.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.util.Set;
/** Scheduled dispatcher that polls the Redis delay queue and sends ready jobs to Kafka. */
@Service
public class RedisDispatcherService {
    private static final Logger log = LoggerFactory.getLogger(RedisDispatcherService.class);
    @Autowired
    private RedisDelayQueueService redisQueue;
    @Autowired
    private JobRepository repo;
    @Autowired
    private KafkaProducerService producer;
    /** Polls Redis every second for jobs that are ready to dispatch. */
    @Scheduled(fixedDelay = 1000)
    public void dispatch() {
        long now = System.currentTimeMillis();
        Set<String> jobIds = redisQueue.getReadyJobs(now);
        if (jobIds == null || jobIds.isEmpty()) {
            return;
        }
        log.info("Dispatcher found {} ready jobs", jobIds.size());
        for (String jobIdStr : jobIds) {
            try {
                Long jobId = Long.parseLong(jobIdStr);
                Job job = repo.findById(jobId).orElse(null);
                if (job == null) {
                    log.warn("Job {} not found in DB, removing from Redis", jobId);
                    redisQueue.removeJob(jobIdStr);
                    continue;
                }
                if (JobStatus.SCHEDULED != job.getStatus()) {
                    log.warn("Skipping job {} due to invalid state: {}", jobId, job.getStatus());
                    redisQueue.removeJob(jobIdStr);
                    continue;
                }
                job.setStatus(JobStatus.QUEUED);
                repo.save(job);
                producer.sendJob(jobIdStr);
                log.info("Job {} dispatched to Kafka", jobId);
                redisQueue.removeJob(jobIdStr);
            } catch (Exception e) {
                log.error("Error dispatching job {}: {}", jobIdStr, e.getMessage(), e);
            }
        }
    }
}