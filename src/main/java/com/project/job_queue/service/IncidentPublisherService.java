package com.project.job_queue.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.job_queue.dto.IncidentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Publishes structured incident events to Kafka for the healing orchestrator. */
@Service
public class IncidentPublisherService {
    private static final Logger log = LoggerFactory.getLogger(IncidentPublisherService.class);
    private static final String TOPIC = "incident-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final boolean enabled;

    public IncidentPublisherService(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${incident.publisher.enabled:true}") boolean enabled) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.enabled = enabled;
    }

    /** Publishes a structured incident event to the incident-topic Kafka topic. */
    public void publish(String type, String severity, String title, String jobId,
                        String jobType, String evidence, Map<String, String> attributes) {
        if (!enabled) {
            return;
        }
        IncidentEvent event = new IncidentEvent();
        event.setIncidentId(UUID.randomUUID().toString());
        event.setType(type);
        event.setSeverity(severity);
        event.setTitle(title);
        event.setJobId(jobId);
        event.setJobType(jobType);
        event.setEvidence(evidence);
        event.setSource("job-queue-app");
        event.setTimestamp(Instant.now());
        event.setAttributes(attributes);

        try {
            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(TOPIC, event.getIncidentId(), payload);
            log.info("Published incident type={} severity={} jobId={}", type, severity, jobId);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize incident event type={}", type, e);
        }
    }
}
