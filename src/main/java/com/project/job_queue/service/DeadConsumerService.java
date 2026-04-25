package com.project.job_queue.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class DeadConsumerService {

    @KafkaListener(
            topics = "dead-topic",
            groupId = "dead-group"
    )
    public void dead(String message) {

        Long jobId = Long.parseLong(message);

        System.out.println("Job " + jobId + " permanently failed");
    }
}