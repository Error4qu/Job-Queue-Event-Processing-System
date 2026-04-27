package com.project.job_queue.executer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
/** Factory that resolves the correct executor implementation for a given job type. */
@Component
public class ExecutorFactory {
    private static final Logger log = LoggerFactory.getLogger(ExecutorFactory.class);
    private final EmailExecutor emailExecutor;
    private final ApiExecutor apiExecutor;
    private final LogExecutor logExecutor;
    /** Constructs the factory with all available executor implementations. */
    public ExecutorFactory(EmailExecutor emailExecutor, ApiExecutor apiExecutor,
                           LogExecutor logExecutor) {
        this.emailExecutor = emailExecutor;
        this.apiExecutor = apiExecutor;
        this.logExecutor = logExecutor;
    }
    /** Returns the executor instance matching the given job type string. */
    public JobExecutor getExecutor(String type) {
        log.debug("Resolving executor for type={}", type);
        return switch (type) {
            case "EMAIL" -> emailExecutor;
            case "API" -> apiExecutor;
            case "LOG" -> logExecutor;
            default -> throw new IllegalArgumentException("Invalid job type: " + type);
        };
    }
}