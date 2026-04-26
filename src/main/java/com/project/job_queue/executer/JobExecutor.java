package com.project.job_queue.executor;

import com.project.job_queue.model.Job;

public interface JobExecutor {
    void execute(Job job) throws Exception;
}