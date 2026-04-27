package com.project.job_queue.service;
import com.project.job_queue.executer.ExecutorFactory;
import com.project.job_queue.executer.JobExecutor;
import com.project.job_queue.model.Job;
import com.project.job_queue.model.JobStatus;
import com.project.job_queue.repository.JobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
/** Unit tests for KafkaConsumerService covering all consume paths. */
@ExtendWith(MockitoExtension.class)
class KafkaConsumerServiceTest {
    @Mock
    private JobRepository repo;
    @Mock
    private ExecutorFactory executorFactory;
    @Mock
    private RedisService redisService;
    @Mock
    private RateLimiterService rateLimiter;
    @Mock
    private Acknowledgment ack;
    @Mock
    private JobExecutor jobExecutor;
    private KafkaConsumerService consumer;
    @BeforeEach
    void setUp() {
        consumer = new KafkaConsumerService(repo, executorFactory, redisService, rateLimiter);
    }
    @Test
    void consume_invalidMessage_shouldAckAndReturn() {
        consumer.consume("not-a-number", ack);
        verify(ack).acknowledge();
        verifyNoInteractions(repo);
    }
    @Test
    void consume_jobNotFound_shouldAckAndReturn() {
        when(repo.findById(99L)).thenReturn(Optional.empty());
        consumer.consume("99", ack);
        verify(ack).acknowledge();
        verify(repo, never()).save(any());
    }
    @Test
    void consume_cancelledJob_shouldSetCancelledAndClearRedis() {
        Job job = createJob(1L, JobStatus.QUEUED);
        when(repo.findById(1L)).thenReturn(Optional.of(job));
        when(redisService.isCancelled(1L)).thenReturn(true);
        consumer.consume("1", ack);
        assertEquals(JobStatus.CANCELLED, job.getStatus());
        verify(repo).save(job);
        verify(redisService).clearCancellation(1L);
        verify(ack).acknowledge();
    }
    @Test
    void consume_rateLimited_shouldSetRetryStatus() {
        Job job = createJob(2L, JobStatus.QUEUED);
        when(repo.findById(2L)).thenReturn(Optional.of(job));
        when(redisService.isCancelled(2L)).thenReturn(false);
        when(rateLimiter.allow("LOG", 5)).thenReturn(false);
        consumer.consume("2", ack);
        assertEquals(JobStatus.RETRY, job.getStatus());
        assertNotNull(job.getNextRetryTime());
        verify(repo).save(job);
        verify(ack).acknowledge();
    }
    @Test
    void consume_validJob_shouldExecuteAndComplete() throws Exception {
        Job job = createJob(3L, JobStatus.QUEUED);
        when(repo.findById(3L)).thenReturn(Optional.of(job));
        when(redisService.isCancelled(3L)).thenReturn(false);
        when(rateLimiter.allow("LOG", 5)).thenReturn(true);
        when(executorFactory.getExecutor("LOG")).thenReturn(jobExecutor);
        consumer.consume("3", ack);
        assertEquals(JobStatus.COMPLETED, job.getStatus());
        verify(jobExecutor).execute(job);
        verify(repo, times(2)).save(job);
        verify(ack).acknowledge();
    }
    @Test
    void consume_nonQueuedStatus_shouldAckWithoutExecution() {
        Job job = createJob(4L, JobStatus.PROCESSING);
        when(repo.findById(4L)).thenReturn(Optional.of(job));
        when(redisService.isCancelled(4L)).thenReturn(false);
        when(rateLimiter.allow("LOG", 5)).thenReturn(true);
        consumer.consume("4", ack);
        assertEquals(JobStatus.PROCESSING, job.getStatus());
        verifyNoInteractions(executorFactory);
        verify(ack).acknowledge();
    }
    @Test
    void consume_executionFailure_shouldRetryWithBackoff() throws Exception {
        Job job = createJob(5L, JobStatus.QUEUED);
        job.setRetryCount(0);
        when(repo.findById(5L)).thenReturn(Optional.of(job));
        when(redisService.isCancelled(5L)).thenReturn(false);
        when(rateLimiter.allow("LOG", 5)).thenReturn(true);
        when(executorFactory.getExecutor("LOG")).thenReturn(jobExecutor);
        doThrow(new RuntimeException("Execution failed")).when(jobExecutor).execute(job);
        consumer.consume("5", ack);
        assertEquals(JobStatus.RETRY, job.getStatus());
        assertEquals(1, job.getRetryCount());
        assertNotNull(job.getNextRetryTime());
        verify(ack).acknowledge();
    }
    @Test
    void consume_maxRetryExceeded_shouldSetFailed() throws Exception {
        Job job = createJob(6L, JobStatus.QUEUED);
        job.setRetryCount(5);
        when(repo.findById(6L)).thenReturn(Optional.of(job));
        when(redisService.isCancelled(6L)).thenReturn(false);
        when(rateLimiter.allow("LOG", 5)).thenReturn(true);
        when(executorFactory.getExecutor("LOG")).thenReturn(jobExecutor);
        doThrow(new RuntimeException("fail")).when(jobExecutor).execute(job);
        consumer.consume("6", ack);
        assertEquals(JobStatus.FAILED, job.getStatus());
        verify(ack).acknowledge();
    }
    private Job createJob(Long id, JobStatus status) {
        Job job = new Job();
        try {
            var idField = Job.class.getDeclaredField("id");
            idField.setAccessible(true);
            idField.set(job, id);
        } catch (Exception ignored) {}
        job.setType("LOG");
        job.setStatus(status);
        job.setRetryCount(0);
        return job;
    }
}
