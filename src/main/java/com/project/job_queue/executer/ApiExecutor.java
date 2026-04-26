package com.project.job_queue.executer;
import com.project.job_queue.model.Job;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.function.client.WebClient;
@Component
public class ApiExecutor implements com.project.job_queue.executor.JobExecutor {
    private final WebClient webClient = WebClient.create();
    @Override
    public void execute(Job job) {
        String url = job.getPayload();
        String response = webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(String.class)
                .block();
        System.out.println("API RESPONSE: " + response);
    }
}