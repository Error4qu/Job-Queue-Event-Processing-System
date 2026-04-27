package com.project.job_queue.service;
import com.project.job_queue.model.Job;
import com.project.job_queue.model.JobStatus;
import com.project.job_queue.repository.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.util.List;
/** Scheduled watcher that picks up PENDING jobs and pushes them into the Redis delay queue. */
@Service
public class WatcherService {
    private static final Logger log = LoggerFactory.getLogger(WatcherService.class);
    private final JobRepository repo;
    private final RedisDelayQueueService redisQueue;
    private final StringRedisTemplate redis;
    /** Constructs the watcher with required dependencies. */
    public WatcherService(JobRepository repo, RedisDelayQueueService redisQueue,
                          StringRedisTemplate redis) {
        this.repo = repo;
        this.redisQueue = redisQueue;
        this.redis = redis;
    }
    /** Runs every 20 seconds to schedule pending jobs and detect watcher gaps. */
    @Scheduled(fixedDelay = 20000)
    public void watch() {
        long now = System.currentTimeMillis();
        String last = redis.opsForValue().get("watcher:lastRun");
        redis.opsForValue().set("watcher:lastRun", String.valueOf(now));
        if (last != null && now - Long.parseLong(last) > 25000) {
            log.warn("Watcher gap detected, triggering recovery");
            recoverMissedJobs(now);
        }
        List<Job> jobs = repo.findWindow(
                now,
                now + 20000,
                PageRequest.of(0, 200)
        );
        for (Job job : jobs) {
            if (JobStatus.PENDING != job.getStatus()) continue;
            job.setStatus(JobStatus.SCHEDULED);
            repo.save(job);
            redisQueue.scheduleJob(job.getId(), job.getScheduleTime());
            log.info("Job scheduled jobId={} scheduleTime={}", job.getId(), job.getScheduleTime());
        }
    }
    /** Recovers jobs that may have been missed during a watcher outage. */
    private void recoverMissedJobs(long now) {
        long start = now - 60000;
        List<Job> jobs = repo.findPendingOrScheduledInRange(
                start,
                now,
                PageRequest.of(0, 500)
        );
        for (Job job : jobs) {
            if (JobStatus.PENDING != job.getStatus() &&
                    JobStatus.SCHEDULED != job.getStatus()) continue;
            redisQueue.scheduleJob(job.getId(), job.getScheduleTime());
            if (JobStatus.PENDING == job.getStatus()) {
                job.setStatus(JobStatus.SCHEDULED);
                repo.save(job);
            }
            log.info("Recovered missed jobId={}", job.getId());
        }
    }
}