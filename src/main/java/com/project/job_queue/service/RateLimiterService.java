package com.project.job_queue.service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import java.util.Collections;
/** Redis-backed sliding window rate limiter using a Lua script for atomic operations. */
@Service
public class RateLimiterService {
    private static final Logger log = LoggerFactory.getLogger(RateLimiterService.class);
    private final StringRedisTemplate redis;
    String lua = """
    local key = KEYS[1]
    local now = tonumber(ARGV[1])
    local window = tonumber(ARGV[2])
    local limit = tonumber(ARGV[3])
    redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
    local count = redis.call('ZCARD', key)
    if count >= limit then
        return 0
    end
    redis.call('ZADD', key, now, now)
    redis.call('PEXPIRE', key, window)
    return 1
    """;
    private final DefaultRedisScript<Long> script =
            new DefaultRedisScript<>(lua, Long.class);
    /** Constructs the rate limiter with the Redis template dependency. */
    public RateLimiterService(StringRedisTemplate redis) {
        this.redis = redis;
    }
    /** Checks if a request for the given type is allowed under the rate limit. */
    public boolean allow(String type, int limitPerSec) {
        String key = "rate:" + type;
        long now = System.currentTimeMillis();
        Long result = redis.execute(
                script,
                Collections.singletonList(key),
                String.valueOf(now),
                "1000",
                String.valueOf(limitPerSec)
        );
        boolean allowed = result != null && result == 1;
        if (!allowed) {
            log.warn("Rate limit exceeded for type={}", type);
        }
        return allowed;
    }
}