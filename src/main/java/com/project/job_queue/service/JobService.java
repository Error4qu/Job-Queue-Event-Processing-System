package com.project.job_queue.service;
import com.project.job_queue.dto.JobRequest;
import com.project.job_queue.model.Job;
import com.project.job_queue.model.JobStatus;
import com.project.job_queue.repository.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.time.*;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
/** Service responsible for creating jobs and routing them to Kafka or the scheduler. */
@Service
public class JobService {
    private static final Logger log = LoggerFactory.getLogger(JobService.class);
    private static final long IMMEDIATE_THRESHOLD = 60 * 1000;
    private final JobRepository repo;
    private final KafkaProducerService producer;
    /** Constructs the service with required repository and producer dependencies. */
    public JobService(JobRepository repo, KafkaProducerService producer) {
        this.repo = repo;
        this.producer = producer;
    }
    /** Creates a new job, persists it, and dispatches immediately if within threshold. */
    public Job createJob(JobRequest request) {
        Job job = new Job();
        job.setIdempotencyKey(UUID.randomUUID().toString());
        job.setPayload(request.getPayload());
        job.setStatus(JobStatus.PENDING);
        job.setRetryCount(0);
        job.setEmail(request.getEmail());
        job.setMessage(request.getMessage());
        job.setType(request.getType());
        long scheduleTime;
        if (request.getTime() != null && !request.getTime().isEmpty()) {
            String[] parts = request.getTime().split(":");
            int hour = Integer.parseInt(parts[0]);
            int minute = Integer.parseInt(parts[1]);
            ZoneId zone = ZoneId.of("Asia/Kolkata");
            LocalDateTime dateTime = LocalDateTime.of(
                    LocalDate.now(zone),
                    LocalTime.of(hour, minute)
            );
            if (dateTime.atZone(zone).toInstant().isBefore(Instant.now())) {
                dateTime = dateTime.plusDays(1);
            }
            scheduleTime = dateTime
                    .atZone(zone)
                    .toInstant()
                    .toEpochMilli();
        } else {
            scheduleTime = System.currentTimeMillis();
        }
        log.info("Job schedule time={} currentTime={}", scheduleTime, System.currentTimeMillis());
        job.setScheduleTime(scheduleTime);
        Job saved = repo.save(job);
        long now = System.currentTimeMillis();
        if (scheduleTime - now <= IMMEDIATE_THRESHOLD) {
            log.info("Immediate job dispatched to Kafka jobId={}", saved.getId());
            saved.setStatus(JobStatus.QUEUED);
            repo.save(saved);
            producer.sendJob(saved.getId().toString());
        }
        return saved;
    }
    /** Retrieves a job by its ID. */
    public Job getJob(Long id) {
        return repo.findById(id).orElse(null);
    }

    /** Returns a snapshot of job counts grouped by status plus a total. */
    public Map<String, Long> getStats() {
        Map<String, Long> stats = new LinkedHashMap<>();
        for (JobStatus s : JobStatus.values()) {
            stats.put(s.name(), repo.countByStatus(s));
        }
        stats.put("TOTAL", repo.count());
        return stats;
    }
}