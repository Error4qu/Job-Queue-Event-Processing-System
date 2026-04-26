package com.project.job_queue.executer;

import com.project.job_queue.executer.ApiExecutor;
import com.project.job_queue.executer.EmailExecutor;
import com.project.job_queue.executer.LogExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
@Component
public class ExecutorFactory {

    @Autowired
    private EmailExecutor emailExecutor;

    @Autowired
    private ApiExecutor apiExecutor;

    @Autowired
    private LogExecutor logExecutor;

    public com.project.job_queue.executer.JobExecutor getExecutor(String type) {
        return switch (type) {
            case "EMAIL" -> emailExecutor;
            case "API" -> apiExecutor;
            case "LOG" -> logExecutor;
            default -> throw new RuntimeException("Invalid job type");
        };
    }
}