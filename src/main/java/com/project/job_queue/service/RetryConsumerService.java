package com.project.job_queue.service;
import org.springframework.kafka.support.Acknowledgment;

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
            concurrency = "1"
    )
    public void retry(String message, Acknowledgment ack) {
        Long jobId = Long.parseLong(message);
        Job job = repo.findById(jobId).orElse(null);
        if (job == null) {
            System.out.println("⚠️ Job not found, skipping: " + jobId);
            ack.acknowledge();
            return;
        }
        long now = System.currentTimeMillis();
        if (now < job.getNextRetryTime()) {
            System.out.println("⏳ Not ready yet for retry: " + jobId);
            ack.acknowledge();
            return;
        }
        System.out.println("Retrying job " + jobId);
        job.setStatus("QUEUED");
        repo.save(job);
        producer.sendJob(message);
        ack.acknowledge();
    }
}