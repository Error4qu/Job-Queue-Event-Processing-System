package com.project.job_queue.service;

import com.project.job_queue.model.Job;
import org.springframework.beans.factory.annotation.Autowired;
import com.project.job_queue.repository.JobRepository;
import org.springframework.stereotype.Service;

@Service
public class JobService {
    @Autowired
    private JobRepository repo;
    @Autowired
    private KafkaProducerService producer;
    public Job createJob(Job job) {
        job.setStatus("PENDING");
        Job saved = repo.save(job);
        producer.sendJob(saved.getId().toString());
        return saved;
    }
}
