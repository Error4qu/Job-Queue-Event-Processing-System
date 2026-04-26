package com.project.job_queue.executor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ExecutorFactory {

    @Autowired
    private com.project.job_queue.executor.EmailExecutor emailExecutor;

    public com.project.job_queue.executor.JobExecutor getExecutor(String type) {

        if (type == null) {
            throw new RuntimeException("Job type is null");
        }

        return switch (type) {
            case "EMAIL" -> emailExecutor;
            default -> throw new RuntimeException("Unknown job type: " + type);
        };
    }
}