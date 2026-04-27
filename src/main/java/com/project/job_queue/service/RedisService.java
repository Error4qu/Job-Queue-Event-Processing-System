package com.project.job_queue.service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import java.time.Duration;
/** Service for managing job cancellation flags in Redis with TTL-based expiry. */
@Service
public class RedisService {
    private static final Logger log = LoggerFactory.getLogger(RedisService.class);
    private static final Duration CANCEL_TTL = Duration.ofMinutes(10);
    private final StringRedisTemplate redis;
    /** Constructs the service with the Redis template dependency. */
    public RedisService(StringRedisTemplate redis) {
        this.redis = redis;
    }
    /** Sets the cancellation flag for a job in Redis with a 10 minute TTL. */
    public void cancelJob(Long jobId) {
        log.info("Marking job as cancelled in Redis jobId={}", jobId);
        redis.opsForValue().set("job:" + jobId, "CANCELLED", CANCEL_TTL);
    }
    /** Checks whether a job has been flagged as cancelled in Redis. */
    public boolean isCancelled(Long jobId) {
        String val = redis.opsForValue().get("job:" + jobId);
        return "CANCELLED".equals(val);
    }
    /** Removes the cancellation flag from Redis after it has been processed. */
    public void clearCancellation(Long jobId) {
        log.info("Clearing cancellation flag from Redis jobId={}", jobId);
        redis.delete("job:" + jobId);
    }
}