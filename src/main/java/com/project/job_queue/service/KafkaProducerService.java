package com.project.job_queue.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    public void sendRetry(String message) {
        kafkaTemplate.send("retry-topic", message);
    }

    public void sendDead(String message) {
        kafkaTemplate.send("dead-topic", message);
    }
    public void sendJob(String message) {
        kafkaTemplate.send("job-topic", message);
    }
}