package com.project.job_queue.service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
/** Service for publishing job messages to Kafka topic. */
@Service
public class KafkaProducerService {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    /** Sends a job ID message to the job-topic Kafka topic. */
    public void sendJob(String message) {
        log.info("Publishing to Kafka topic=job-topic message={}", message);
        kafkaTemplate.send("job-topic", message);
    }
}