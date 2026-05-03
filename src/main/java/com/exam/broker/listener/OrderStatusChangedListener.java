package com.exam.broker.listener;

import com.exam.broker.model.*;
import com.exam.broker.repository.EnvioRepository;
import com.exam.broker.repository.NotificationLogRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;
import java.util.Map;

@Service
public class OrderStatusChangedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderStatusChangedListener.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private EnvioRepository envioRepository;

    @Autowired
    private NotificationLogRepository notificationLogRepository;

    @Autowired
    private JavaMailSender mailSender;

    @KafkaListener(topics = "order_status_changed_events", groupId = "broker-status-group")
    public void listen(String payload) {
        log.info("Received order_status_changed_events: {}", payload);
        try {
            Map<String, Object> event = objectMapper.readValue(payload, Map.class);
            String ordenId = (String) event.get("ordenId");
            String nuevoEstado = (String) event.get("nuevoEstado");

            // Workflow: ACTUALIZAR ESTATUS DE ORDEN - Paso 2
            // 1. Enviar correo al cliente
            sendNotificationEmail(ordenId, nuevoEstado);

            // 2. Condición: Estatus = Pagado?
            if ("PAGADA".equalsIgnoreCase(nuevoEstado)) {
                log.info("Order {} status changed to PAGADA. Registering for shipping.", ordenId);
                
                // Idempotency check: Ensure we don't add to shippings twice if status update is retried
                if (envioRepository.findByEstado("PENDIENTE_ENVIO").stream().noneMatch(e -> e.getOrdenId().equals(ordenId))) {
                    Envio envio = new Envio();
                    envio.setOrdenId(ordenId);
                    envio.setClienteEmail("dinocodeadvisor@gmail.com"); // Mock
                    envioRepository.save(envio);
                }
            }

        } catch (Exception e) {
            log.error("Error processing order status changed event: {}", e.getMessage());
        }
    }

    private void sendNotificationEmail(String ordenId, String estado) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo("dinocodeadvisor@gmail.com");
        message.setSubject("🔔 Actualización de Estado - Orden " + ordenId);
        message.setText("Tu orden " + ordenId + " ha cambiado de estado a: " + estado);
        
        try {
            mailSender.send(message);
            log.info("Status update email sent for order {}", ordenId);
        } catch (Exception e) {
            log.error("Failed to send status update email: {}", e.getMessage());
        }
    }
}
