package com.project.job_queue.service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import java.util.Collections;
@Service
public class RateLimiterService {
    @Autowired
    private StringRedisTemplate redis;
    private final String lua = """
    local key = KEYS[1]
    local limit = tonumber(ARGV[1])
    local ttl = tonumber(ARGV[2])

    local current = redis.call("GET", key)

    if current and tonumber(current) >= limit then
        return 0
    else
        current = redis.call("INCR", key)
        if current == 1 then
            redis.call("PEXPIRE", key, ttl)
        end
        return 1
    end
    """;
    public boolean allow(String type, int limitPerSec) {
        String key = "rate:" + type;
        Long result = redis.execute(
                new DefaultRedisScript<>(lua, Long.class),
                Collections.singletonList(key),
                String.valueOf(limitPerSec),
                "1000"
        );
        return result != null && result == 1;
    }
}