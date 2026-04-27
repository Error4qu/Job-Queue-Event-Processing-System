package com.project.job_queue.service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import java.util.Set;
/** Service for managing the Redis sorted-set based delay queue for scheduled jobs. */
@Service
public class RedisDelayQueueService {
    private static final Logger log = LoggerFactory.getLogger(RedisDelayQueueService.class);
    private static final String KEY = "scheduled_jobs";
    @Autowired
    private StringRedisTemplate redis;
    /** Adds a job to the delay queue with its scheduled execution time as score. */
    public void scheduleJob(Long jobId, long scheduleTime) {
        log.info("Scheduling job in Redis delay queue jobId={} scheduleTime={}", jobId, scheduleTime);
        redis.opsForZSet().add(KEY, jobId.toString(), scheduleTime);
    }
    /** Returns all jobs whose schedule time has passed. */
    public Set<String> getReadyJobs(long now) {
        return redis.opsForZSet().rangeByScore(KEY, 0, now);
    }
    /** Removes a job from the delay queue after dispatch. */
    public void removeJob(String jobId) {
        log.debug("Removing job from Redis delay queue jobId={}", jobId);
        redis.opsForZSet().remove(KEY, jobId);
    }
}