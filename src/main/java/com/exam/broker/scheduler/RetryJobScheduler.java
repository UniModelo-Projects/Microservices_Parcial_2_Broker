package com.exam.broker.scheduler;

import com.exam.broker.handler.*;
import com.exam.broker.model.RetryJob;
import com.exam.broker.repository.RetryJobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import jakarta.mail.internet.MimeMessage;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.util.List;

@Component
public class RetryJobScheduler {

    @Autowired
    private RetryJobRepository repository;

    @Autowired
    private CreationHandler creationHandler;

    @Autowired
    private EmailHandler emailHandler;

    @Autowired
    private UpdateHandler updateHandler;

    @Autowired
    private MongoHandler mongoHandler;

    @Autowired
    private JavaMailSender mailSender;

    @Scheduled(fixedRate = 10000)
    public void processPendingJobs() {
        List<RetryJob> jobs = repository.findByStatusAndNextRunAtBefore("PENDING", LocalDateTime.now());
        if (jobs.isEmpty()) return;

        System.out.println("--- Starting scheduler run. Found " + jobs.size() + " jobs ---");

        // Build chain: A -> B -> C -> D
        creationHandler.setNext(emailHandler);
        emailHandler.setNext(updateHandler);
        updateHandler.setNext(mongoHandler);

        for (RetryJob job : jobs) {
            try {
                creationHandler.handle(job);
            } catch (Exception e) {
                System.err.println("Chain failed for job: " + job.getId() + ". Error: " + e.getMessage());

                // Requirement: Send failure email if any step fails
                sendFailureEmail(job.getTopicType(), e.getMessage());

                job.setRetryCount(job.getRetryCount() + 1);
                job.setLastError(e.getMessage());

                if (job.getRetryCount() >= job.getMaxRetries()) {
                    job.setStatus("FAILED");
                    if (!"SUCCESS".equals(job.getStepAStatus())) {
                        sendFailureEmail(job.getTopicType(), e.getMessage());
                    }
                } else {
                    // Exponential Backoff
                    long secondsToAdd = (long) (10 * Math.pow(2, job.getRetryCount()));
                    job.setNextRunAt(LocalDateTime.now().plusSeconds(secondsToAdd));
                }
                repository.save(job);
            }
        }
    }

    private void sendFailureEmail(String topicType, String error) {
        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");

            helper.setTo("dinocodeadvisor@gmail.com");
            helper.setSubject("❌ " + topicType + " Creation Failed");
            helper.setText("We were unable to process your " + topicType.toLowerCase() + " after several attempts.\n\nFinal Error: " + error);
            
            mailSender.send(message);
        } catch (Exception e) {
            System.err.println("Failed to send failure email: " + e.getMessage());
        }
    }
}
