package com.project.job_queue.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.job_queue.model.Job;
import com.project.job_queue.model.JobStatus;
import com.project.job_queue.service.JobService;
import com.project.job_queue.service.RedisService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.lang.reflect.Field;
import java.util.Map;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Web layer tests for JobController request validation and route behavior. */
@WebMvcTest(JobController.class)
class JobControllerTest {
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private JobService jobService;

    @MockBean
    private RedisService redisService;

    @Test
    void createJob_validRequest_returnsCreatedJob() throws Exception {
        Job job = job(1L, "LOG", JobStatus.QUEUED);
        when(jobService.createJob(org.mockito.ArgumentMatchers.any())).thenReturn(job);

        mockMvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Map.of(
                                "type", "LOG",
                                "payload", "test-payload"
                        ))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.type").value("LOG"))
                .andExpect(jsonPath("$.status").value("QUEUED"));
    }

    @Test
    void createJob_missingType_returnsValidationError() throws Exception {
        mockMvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Map.of(
                                "payload", "missing-type"
                        ))))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("Validation Failed"))
                .andExpect(jsonPath("$.fieldErrors.type").exists());
    }

    @Test
    void createJob_invalidTime_returnsValidationError() throws Exception {
        mockMvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Map.of(
                                "type", "LOG",
                                "payload", "bad-time",
                                "time", "25:99"
                        ))))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.fieldErrors.time").value("Time must be in HH:mm format"));
    }

    @Test
    void cancelJob_setsCancellationFlag() throws Exception {
        mockMvc.perform(post("/jobs/42/cancel"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").value("Job 42 cancelled"));

        verify(redisService).cancelJob(42L);
    }

    @Test
    void getJob_returnsJob() throws Exception {
        when(jobService.getJob(7L)).thenReturn(job(7L, "API", JobStatus.RETRY));

        mockMvc.perform(get("/jobs/7"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(7))
                .andExpect(jsonPath("$.type").value("API"))
                .andExpect(jsonPath("$.status").value("RETRY"));
    }

    @Test
    void getStats_returnsStatusCounts() throws Exception {
        when(jobService.getStats()).thenReturn(Map.of(
                "PENDING", 1L,
                "COMPLETED", 2L,
                "TOTAL", 3L
        ));

        mockMvc.perform(get("/jobs/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.PENDING").value(1))
                .andExpect(jsonPath("$.COMPLETED").value(2))
                .andExpect(jsonPath("$.TOTAL").value(3));
    }

    private Job job(Long id, String type, JobStatus status) throws Exception {
        Job job = new Job();
        Field idField = Job.class.getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(job, id);
        job.setType(type);
        job.setStatus(status);
        return job;
    }
}
