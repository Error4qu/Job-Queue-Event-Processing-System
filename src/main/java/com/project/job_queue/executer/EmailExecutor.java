package com.project.job_queue.executer;
import com.project.job_queue.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;
import java.util.Properties;
/** Executor that sends an email using the job email and message fields. */
@Component
public class EmailExecutor implements JobExecutor {
    private static final Logger log = LoggerFactory.getLogger(EmailExecutor.class);
    @Autowired
    private JavaMailSender mailSender;
    /** Sends an email to the recipient specified in the job. */
    @Override
    public void execute(Job job) {
        Properties props = System.getProperties();
        props.put("mail.smtp.ssl.trust", "*");
        SimpleMailMessage mail = new SimpleMailMessage();
        mail.setTo(job.getEmail());
        mail.setSubject("Reminder");
        mail.setText(job.getMessage());
        mailSender.send(mail);
        log.info("Email sent to={} jobId={}", job.getEmail(), job.getId());
    }
}