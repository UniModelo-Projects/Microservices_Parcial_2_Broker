package com.exam.broker.scheduler;

import com.exam.broker.model.Envio;
import com.exam.broker.model.NotificationLog;
import com.exam.broker.repository.EnvioRepository;
import com.exam.broker.repository.NotificationLogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.util.List;

@Component
public class ShippingScheduler {

    private static final Logger log = LoggerFactory.getLogger(ShippingScheduler.class);

    @Autowired
    private EnvioRepository envioRepository;

    @Autowired
    private NotificationLogRepository notificationLogRepository;

    @Autowired
    private JavaMailSender mailSender;

    @Scheduled(fixedRate = 10000)
    public void processPendingShippings() {
        List<Envio> pendingEnvios = envioRepository.findByEstado("PENDIENTE_ENVIO");
        if (pendingEnvios.isEmpty()) return;

        log.info("Processing {} pending shippings", pendingEnvios.size());

        for (Envio envio : pendingEnvios) {
            try {
                // Idempotency: Verify if final email already sent for this order
                if (notificationLogRepository.findByOrdenIdAndTipoEvento(envio.getOrdenId(), "ENVIO_CONFIRMADO").isPresent()) {
                    log.info("Shipping email already sent for order {}, marking as processed", envio.getOrdenId());
                    envio.setEstado("PROCESADO");
                    envio.setFechaProcesado(LocalDateTime.now());
                    envioRepository.save(envio);
                    continue;
                }

                // Send Final Email (Workflow Paso 3)
                sendShippingEmail(envio);

                // Update Envio status
                envio.setEstado("PROCESADO");
                envio.setFechaProcesado(LocalDateTime.now());
                envioRepository.save(envio);

                // Log notification
                notificationLogRepository.save(new NotificationLog(envio.getOrdenId(), "ENVIO_CONFIRMADO"));

            } catch (Exception e) {
                log.error("Failed to process shipping for order {}: {}", envio.getOrdenId(), e.getMessage());
            }
        }
    }

    private void sendShippingEmail(Envio envio) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo(envio.getClienteEmail());
        message.setSubject("🚚 Envío Confirmado - Orden " + envio.getOrdenId());
        message.setText("Tu orden ha sido procesada por nuestro almacén y está en camino.\n\nID de Orden: " + envio.getOrdenId() + "\n\n¡Gracias por tu compra!");
        
        mailSender.send(message);
        log.info("Shipping confirmation email sent for order {}", envio.getOrdenId());
    }
}
