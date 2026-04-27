package com.project.job_queue.executer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
/** Factory that resolves the correct executor implementation for a given job type. */
@Component
public class ExecutorFactory {
    private static final Logger log = LoggerFactory.getLogger(ExecutorFactory.class);
    @Autowired
    private EmailExecutor emailExecutor;
    @Autowired
    private ApiExecutor apiExecutor;
    @Autowired
    private LogExecutor logExecutor;
    /** Returns the executor instance matching the given job type string. */
    public JobExecutor getExecutor(String type) {
        log.debug("Resolving executor for type={}", type);
        return switch (type) {
            case "EMAIL" -> emailExecutor;
            case "API" -> apiExecutor;
            case "LOG" -> logExecutor;
            default -> throw new RuntimeException("Invalid job type");
        };
    }
}