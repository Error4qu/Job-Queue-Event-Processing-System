package com.project.job_queue.service;

import com.project.job_queue.model.Job;
import com.project.job_queue.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class RetryConsumerService {

    @Autowired
    private JobRepository repo;

    @Autowired
    private KafkaProducerService producer;

    @KafkaListener(
            topics = "retry-topic",
            groupId = "retry-group",
            concurrency = "1" // 🔥 IMPORTANT (controlled retry)
    )
    public void retry(String message) {

        Long jobId = Long.parseLong(message);
        Job job = repo.findById(jobId).orElseThrow();

        long now = System.currentTimeMillis();

        if (now < job.getNextRetryTime()) {
            // not ready → requeue
            producer.sendRetry(message);
            return;
        }

        System.out.println("Retrying job " + jobId);

        // send back to main topic
        producer.sendJob(message);
    }
}