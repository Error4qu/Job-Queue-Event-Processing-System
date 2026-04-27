package com.project.job_queue.executer;
import com.project.job_queue.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
/** Executor that logs the job payload for diagnostic or audit purposes. */
@Component
public class LogExecutor implements JobExecutor {
    private static final Logger log = LoggerFactory.getLogger(LogExecutor.class);
    /** Logs the job payload at INFO level. */
    @Override
    public void execute(Job job) {
        log.info("LOG JOB EXECUTED payload={} jobId={}", job.getPayload(), job.getId());
    }
}
