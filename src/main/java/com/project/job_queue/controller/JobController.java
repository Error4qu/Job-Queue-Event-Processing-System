package com.project.job_queue.controller;

import com.project.job_queue.model.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.project.job_queue.service.JobService;

@RestController
@RequestMapping("/jobs")
public class JobController {

    @Autowired
    private JobService service;

    @PostMapping
    public Job createJob(@RequestBody Job job) {
        return service.createJob(job);
    }
}
