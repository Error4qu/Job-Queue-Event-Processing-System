package com.project.job_queue.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class RedisService {

    @Autowired
    private StringRedisTemplate redis;

    public void cancelJob(Long jobId) {
        redis.opsForValue().set("job:" + jobId, "CANCELLED");
    }

    public boolean isCancelled(Long jobId) {
        String val = redis.opsForValue().get("job:" + jobId);
        return "CANCELLED".equals(val);
    }
}