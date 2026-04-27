package com.project.job_queue.executer;
import com.project.job_queue.model.Job;
/** Contract for all job type executors. */
public interface JobExecutor {
    /** Executes the given job. Implementations handle type-specific processing. */
    void execute(Job job) throws Exception;
}