package com.project.job_queue.repository;
import com.project.job_queue.model.Job;
import com.project.job_queue.model.JobStatus;
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

    /** Finds RETRY jobs within the given time window ordered by next retry time. */
    @Query("""
SELECT j FROM Job j
WHERE j.status = 'RETRY'
AND j.nextRetryTime BETWEEN :start AND :end
ORDER BY j.nextRetryTime ASC
""")
    List<Job> findRetryWindow(Long start, Long end, Pageable pageable);

    /** Finds jobs with PENDING or SCHEDULED status within the given time range. */
    @Query("""
SELECT j FROM Job j
WHERE (j.status = 'PENDING' OR j.status = 'SCHEDULED')
AND j.scheduleTime BETWEEN :start AND :end
""")
    List<Job> findPendingOrScheduledInRange(Long start, Long end, Pageable pageable);

    /** Finds jobs with RETRY status within the given time range. */
    @Query("""
SELECT j FROM Job j
WHERE j.status = 'RETRY'
AND j.nextRetryTime BETWEEN :start AND :end
""")
    List<Job> findRetryInRange(Long start, Long end, Pageable pageable);

    /** Counts jobs matching a specific status. */
    long countByStatus(JobStatus status);

    /** Counts total jobs created in the given epoch-ms time range. */
    @Query("SELECT COUNT(j) FROM Job j WHERE j.scheduleTime BETWEEN :start AND :end")
    long countCreatedInRange(Long start, Long end);

    /** Finds SCHEDULED jobs whose schedule time has passed. */
    @Query("""
SELECT j FROM Job j
WHERE j.status = 'SCHEDULED'
AND j.scheduleTime <= :now
ORDER BY j.scheduleTime ASC
""")
    List<Job> findOverdueScheduled(Long now, Pageable pageable);

    /** Finds RETRY jobs whose next retry time has passed. */
    @Query("""
SELECT j FROM Job j
WHERE j.status = 'RETRY'
AND j.nextRetryTime <= :now
ORDER BY j.nextRetryTime ASC
""")
    List<Job> findRetryDue(Long now, Pageable pageable);
}
