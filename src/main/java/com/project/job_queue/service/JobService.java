package com.project.job_queue.service;

import com.project.job_queue.dto.JobRequest;
import com.project.job_queue.model.Job;
import com.project.job_queue.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.*;

@Service
public class JobService {

    @Autowired
    private JobRepository repo;
    @Autowired
    private KafkaProducerService producer;
    private static final long IMMEDIATE_THRESHOLD = 60 * 1000;
    public Job createJob(JobRequest request) {

        Job job = new Job();
        job.setPayload(request.getPayload());
        job.setStatus("PENDING");
        job.setRetryCount(0);
        job.setEmail(request.getEmail());
        job.setMessage(request.getMessage());
        job.setType(request.getType());
        long scheduleTime;

        // 🟢 If time provided → schedule
        if (request.getTime() != null && !request.getTime().isEmpty()) {

            String[] parts = request.getTime().split(":");
            int hour = Integer.parseInt(parts[0]);
            int minute = Integer.parseInt(parts[1]);

            LocalDateTime dateTime = LocalDateTime.of(
                    LocalDate.now(),
                    LocalTime.of(hour, minute)
            );

            // handle past → tomorrow
            if (dateTime.isBefore(LocalDateTime.now())) {
                dateTime = dateTime.plusDays(1);
            }

            scheduleTime = dateTime
                    .atZone(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();

        } else {
            // 🟢 immediate job
            scheduleTime = System.currentTimeMillis();
        }
        System.out.println("SCHEDULE TIME: " + scheduleTime);
        System.out.println("CURRENT TIME: " + System.currentTimeMillis());
        job.setScheduleTime(scheduleTime);
        Job saved = repo.save(job);
        long now = System.currentTimeMillis();
        if (scheduleTime - now <= IMMEDIATE_THRESHOLD) {
            System.out.println("⚡ Immediate job → sending to Kafka: " + saved.getId());
            producer.sendJob(saved.getId().toString());
            job.setStatus("QUEUED");
        }
        return saved;
    }
}