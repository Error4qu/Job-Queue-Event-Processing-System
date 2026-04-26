package com.project.job_queue.executer;

import com.project.job_queue.model.Job;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
@Slf4j
@Component
public class LogExecutor implements com.project.job_queue.executor.JobExecutor {

    @Override
    public void execute(Job job) {
//        log.info("LOG JOB EXECUTED → payload: {}", job.getPayload());
    }
}
