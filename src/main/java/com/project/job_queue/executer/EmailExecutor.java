package com.project.job_queue.executer;
import com.project.job_queue.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;
/** Executor that sends an email using the job email and message fields. */
@Component
public class EmailExecutor implements JobExecutor {
    private static final Logger log = LoggerFactory.getLogger(EmailExecutor.class);
    private final JavaMailSender mailSender;
    /** Constructs the email executor with the mail sender dependency. */
    public EmailExecutor(JavaMailSender mailSender) {
        this.mailSender = mailSender;
    }
    /** Sends an email to the recipient specified in the job. */
    @Override
    public void execute(Job job) {
        SimpleMailMessage mail = new SimpleMailMessage();
        mail.setTo(job.getEmail());
        mail.setSubject("Reminder");
        mail.setText(job.getMessage());
        mailSender.send(mail);
        log.info("Email sent to={} jobId={}", job.getEmail(), job.getId());
    }
}