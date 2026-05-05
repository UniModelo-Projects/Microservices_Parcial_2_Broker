package com.exam.broker.handler;

import com.exam.broker.model.RetryJob;
import com.exam.broker.repository.RetryJobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import jakarta.mail.internet.MimeMessage;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

@Component
public class EmailHandler extends BaseHandler {

    @Autowired
    private JavaMailSender mailSender;

    @Autowired
    private RetryJobRepository repository;

    @Override
    public void handle(RetryJob job) throws Exception {
        if ("SUCCESS".equals(job.getStepBStatus())) {
            if (next != null) next.handle(job);
            return;
        }

        System.out.println("[Step B] Sending success email for: " + job.getTopicType());

        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        java.util.Map<String, Object> fullPayload = mapper.readValue(job.getPayload(), new com.fasterxml.jackson.core.type.TypeReference<java.util.Map<String, Object>>() {});
        java.util.Map<String, Object> data = (java.util.Map<String, Object>) fullPayload.get("data");

        String orderInfo = "";
        String subject = "✅ Transacción Procesada";
        String body = "Tu solicitud ha sido procesada exitosamente.";

        try {
            switch (job.getTopicType().toUpperCase()) {
                case "PAYMENTS":
                case "PAYMENT":
                    String ordenId = (String) data.get("ordenId");
                    Double monto = data.get("monto") != null ? Double.parseDouble(data.get("monto").toString()) : 0.0;
                    subject = "💰 Pago Procesado Correctamente";
                    body = "Hemos procesado con éxito tu pago por $" + monto + " asociado a la orden: " + ordenId + ".\n\nGracias por tu paciencia mientras regularizábamos la transacción.";
                    break;
                case "ORDERS":
                case "ORDER":
                    String orderId = (String) data.get("id");
                    Double total = data.get("total") != null ? Double.parseDouble(data.get("total").toString()) : 0.0;
                    subject = "📦 Orden Confirmada";
                    body = "Tu orden " + (orderId != null ? orderId : "") + " por un total de $" + total + " ha sido registrada en nuestro sistema.\n\nEstamos preparando todo para ti.";
                    break;
                case "PRODUCTS":
                case "PRODUCT":
                    String nombre = (String) data.get("nombre");
                    subject = "✨ Producto Registrado";
                    body = "El producto '" + nombre + "' ha sido dado de alta correctamente en nuestro catálogo.";
                    break;
            }

            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");

            helper.setTo("dinocodeadvisor@gmail.com");
            helper.setSubject(subject);
            helper.setText(body + "\n\nReferencia del sistema: " + job.getId());

            mailSender.send(message);
            fullPayload.put("sendEmail", java.util.Map.of("status", "SUCCESS", "message", "Email sent correctly"));
        } catch (Exception e) {
            System.err.println("Failed to send email: " + e.getMessage());
            fullPayload.put("sendEmail", java.util.Map.of("status", "FAILED", "message", e.getMessage()));
        }

        job.setPayload(mapper.writeValueAsString(fullPayload));
        job.setStepBStatus("SUCCESS");
        repository.save(job);

        if (next != null) next.handle(job);
    }
}
