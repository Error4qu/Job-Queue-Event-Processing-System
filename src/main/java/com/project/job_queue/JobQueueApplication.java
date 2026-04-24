package com.project.job_queue;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class JobQueueApplication {

	public static void main(String[] args) {
		SpringApplication.run(JobQueueApplication.class, args);
	}

}
