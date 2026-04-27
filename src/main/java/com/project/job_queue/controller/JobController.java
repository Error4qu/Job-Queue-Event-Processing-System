package com.project.job_queue.controller;
import com.project.job_queue.dto.JobRequest;
import com.project.job_queue.model.Job;
import com.project.job_queue.service.RedisService;
import com.project.job_queue.service.JobService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
/** REST controller for job creation and cancellation. */
@RestController
@RequestMapping("/jobs")
public class JobController {
    private static final Logger log = LoggerFactory.getLogger(JobController.class);
    private final JobService service;
    private final RedisService redisService;
    /** Constructs the controller with required service dependencies. */
    public JobController(JobService service, RedisService redisService) {
        this.service = service;
        this.redisService = redisService;
    }
    /** Creates a new job from the validated incoming request payload. */
    @PostMapping
    public Job createJob(@Valid @RequestBody JobRequest job) {
        log.info("Received job creation request type={}", job.getType());
        return service.createJob(job);
    }
    /** Marks a job as cancelled by its ID. */
    @PostMapping("/{id}/cancel")
    public String cancel(@PathVariable Long id) {
        log.info("Received cancellation request jobId={}", id);
        redisService.cancelJob(id);
        return "Job " + id + " cancelled";
    }
}
