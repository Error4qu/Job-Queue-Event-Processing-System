package com.project.job_queue.executer;
import com.project.job_queue.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
/** Executor that performs an HTTP GET call using the job payload as the target URL. */
@Component
public class ApiExecutor implements JobExecutor {
    private static final Logger log = LoggerFactory.getLogger(ApiExecutor.class);
    private final WebClient webClient = WebClient.create();
    /** Executes an API call to the URL specified in the job payload. */
    @Override
    public void execute(Job job) {
        String url = job.getPayload();
        log.info("Calling API url={} jobId={}", url, job.getId());
        String response = webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(String.class)
                .block();
        log.info("API response received jobId={} response={}", job.getId(), response);
    }
}