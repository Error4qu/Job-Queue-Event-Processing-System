package com.project.job_queue.repository;
import com.project.job_queue.model.Job;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.domain.Pageable;
import java.util.List;
/** Repository for Job entity providing scheduled job query methods. */
public interface JobRepository extends JpaRepository<Job,Long> {
    /** Finds PENDING jobs within the given time window ordered by schedule time. */
    @Query("""
SELECT j FROM Job j
WHERE j.status = 'PENDING'
AND j.scheduleTime BETWEEN :start AND :end
ORDER BY j.scheduleTime ASC
""")
    List<Job> findWindow(Long start, Long end, Pageable pageable);
    /** Finds jobs with PENDING or SCHEDULED status within the given time range. */
    @Query("""
SELECT j FROM Job j
WHERE (j.status = 'PENDING' OR j.status = 'SCHEDULED')
AND j.scheduleTime BETWEEN :start AND :end
""")
    List<Job> findPendingOrScheduledInRange(Long start, Long end, Pageable pageable);
}
