package com.project.job_queue.repository;

import com.project.job_queue.model.Job;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface JobRepository extends JpaRepository<Job,Long> {
    List<Job> findByStatusAndScheduleTimeLessThanEqual(String status, long time);
}
