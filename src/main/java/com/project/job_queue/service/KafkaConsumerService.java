package com.project.job_queue.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    @KafkaListener(topics = "job-topic", groupId = "job-group")
    public void consume(String message) {
        System.out.println("Processing job: " + message);
    }
}