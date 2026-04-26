package com.project.job_queue.service;

import com.project.job_queue.model.Job;
import com.project.job_queue.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class WatcherService {

    @Autowired
    private JobRepository repo;

    @Autowired
    private KafkaProducerService producer;

    @Scheduled(fixedDelay = 5000)
    public void pollJobs() {

        long now = System.currentTimeMillis();

        System.out.println("=== WATCHER RUNNING ===");
        System.out.println("NOW: " + now);

        List<Job> jobs = repo.findByStatusAndScheduleTimeLessThanEqual("PENDING", now);

        System.out.println("Jobs found: " + jobs.size());

        for (Job job : jobs) {

            System.out.println("Job ID: " + job.getId());
            System.out.println("ScheduleTime: " + job.getScheduleTime());

            producer.sendJob(job.getId().toString());

            job.setStatus("QUEUED");
            repo.save(job);

            System.out.println("Job sent to Kafka ✅");
        }
    }
}