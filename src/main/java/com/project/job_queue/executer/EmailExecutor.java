package com.project.job_queue.executor;

import com.project.job_queue.model.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class EmailExecutor implements JobExecutor {

    @Autowired
    private JavaMailSender mailSender;

    @Override
    public void execute(Job job) {
        Properties props = System.getProperties();
        props.put("mail.smtp.ssl.trust", "*");
        SimpleMailMessage mail = new SimpleMailMessage();

        mail.setTo(job.getEmail());
        mail.setSubject("Reminder");
        mail.setText(job.getMessage());

        mailSender.send(mail);

        System.out.println("Email sent to " + job.getEmail());
    }
}