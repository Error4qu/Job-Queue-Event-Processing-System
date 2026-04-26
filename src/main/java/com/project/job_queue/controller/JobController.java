package com.project.job_queue.controller;

import com.project.job_queue.dto.JobRequest;
import com.project.job_queue.model.Job;
import com.project.job_queue.service.RedisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import com.project.job_queue.service.JobService;

@RestController
@RequestMapping("/jobs")
public class JobController {

    @Autowired
    private JobService service;

    @Autowired
    private RedisService redisService;

    @PostMapping
    public Job createJob(@RequestBody JobRequest job) {
        return service.createJob(job);
    }

    @PostMapping("/{id}/cancel")
    public String cancel(@PathVariable Long id) {
        redisService.cancelJob(id);
        return "Job " + id + " cancelled";
    }
}
