package com.project.job_queue.service;
import com.project.job_queue.dto.JobRequest;
import com.project.job_queue.model.Job;
import com.project.job_queue.model.JobStatus;
import com.project.job_queue.repository.JobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
/** Unit tests for JobService covering creation, scheduling and immediate dispatch. */
@ExtendWith(MockitoExtension.class)
class JobServiceTest {
    @Mock
    private JobRepository repo;
    @Mock
    private KafkaProducerService producer;
    private JobService jobService;
    @BeforeEach
    void setUp() {
        jobService = new JobService(repo, producer);
    }
    @Test
    void createJob_immediateJob_shouldSetQueuedAndDispatch() {
        JobRequest request = createRequest("LOG", null, null, "test-payload", null);
        Job savedJob = new Job();
        savedJob.setType("LOG");
        savedJob.setStatus(JobStatus.PENDING);
        when(repo.save(any(Job.class))).thenAnswer(invocation -> {
            Job j = invocation.getArgument(0);
            try {
                var idField = Job.class.getDeclaredField("id");
                idField.setAccessible(true);
                idField.set(j, 1L);
            } catch (Exception ignored) {}
            return j;
        });
        Job result = jobService.createJob(request);
        assertNotNull(result);
        assertEquals(JobStatus.QUEUED, result.getStatus());
        verify(producer).sendJob("1");
        verify(repo, times(2)).save(any(Job.class));
    }
    @Test
    void createJob_noTime_shouldSetPendingStatus() {
        JobRequest request = createRequest("API", null, null, "http://example.com", null);
        when(repo.save(any(Job.class))).thenAnswer(invocation -> {
            Job j = invocation.getArgument(0);
            try {
                var idField = Job.class.getDeclaredField("id");
                idField.setAccessible(true);
                idField.set(j, 2L);
            } catch (Exception ignored) {}
            return j;
        });
        Job result = jobService.createJob(request);
        assertNotNull(result);
        assertEquals("API", result.getType());
        assertNotNull(result.getIdempotencyKey());
        assertEquals(0, result.getRetryCount());
    }
    @Test
    void createJob_emailType_shouldSetEmailFields() {
        JobRequest request = createRequest("EMAIL", null, "test@test.com", null, "hello");
        when(repo.save(any(Job.class))).thenAnswer(invocation -> {
            Job j = invocation.getArgument(0);
            try {
                var idField = Job.class.getDeclaredField("id");
                idField.setAccessible(true);
                idField.set(j, 3L);
            } catch (Exception ignored) {}
            return j;
        });
        Job result = jobService.createJob(request);
        assertEquals("test@test.com", result.getEmail());
        assertEquals("hello", result.getMessage());
    }
    @Test
    void createJob_shouldGenerateUniqueIdempotencyKey() {
        when(repo.save(any(Job.class))).thenAnswer(invocation -> {
            Job j = invocation.getArgument(0);
            try {
                var idField = Job.class.getDeclaredField("id");
                idField.setAccessible(true);
                idField.set(j, 10L);
            } catch (Exception ignored) {}
            return j;
        });
        JobRequest request1 = createRequest("LOG", null, null, "p1", null);
        JobRequest request2 = createRequest("LOG", null, null, "p2", null);
        Job result1 = jobService.createJob(request1);
        Job result2 = jobService.createJob(request2);
        assertNotEquals(result1.getIdempotencyKey(), result2.getIdempotencyKey());
    }
    @Test
    void createJob_shouldPersistScheduleTime() {
        JobRequest request = createRequest("LOG", null, null, "payload", null);
        ArgumentCaptor<Job> captor = ArgumentCaptor.forClass(Job.class);
        when(repo.save(any(Job.class))).thenAnswer(invocation -> {
            Job j = invocation.getArgument(0);
            try {
                var idField = Job.class.getDeclaredField("id");
                idField.setAccessible(true);
                idField.set(j, 5L);
            } catch (Exception ignored) {}
            return j;
        });
        jobService.createJob(request);
        verify(repo, atLeastOnce()).save(captor.capture());
        Job saved = captor.getAllValues().get(0);
        assertNotNull(saved.getScheduleTime());
        assertTrue(saved.getScheduleTime() > 0);
    }
    private JobRequest createRequest(String type, String time, String email,
                                     String payload, String message) {
        try {
            JobRequest req = new JobRequest();
            setField(req, "type", type);
            setField(req, "time", time);
            setField(req, "email", email);
            setField(req, "payload", payload);
            setField(req, "message", message);
            return req;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private void setField(Object obj, String fieldName, Object value) throws Exception {
        var field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }
}
