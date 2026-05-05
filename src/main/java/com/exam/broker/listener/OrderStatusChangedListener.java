package com.exam.broker.listener;

import com.exam.broker.model.*;
import com.exam.broker.repository.EnvioRepository;
import com.exam.broker.repository.NotificationLogRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import jakarta.mail.internet.MimeMessage;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;
import java.util.Map;
import java.util.List;

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

    @Autowired
    private com.exam.broker.client.OrderClient orderClient;

    @Autowired
    private com.exam.broker.client.ProductClient productClient;

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
        StringBuilder itemsList = new StringBuilder("\nProductos en tu orden:\n");
        String subject = "🔔 Actualización de Estado - Orden " + ordenId;
        String bodyText = "Tu orden " + ordenId + " ha cambiado de estado a: " + estado;

        if ("PENDIENTE".equalsIgnoreCase(estado)) {
            subject = "📦 Orden Confirmada - " + ordenId;
            bodyText = "¡Gracias por tu compra! Tu orden " + ordenId + " ha sido registrada exitosamente y está siendo procesada.";
        }

        try {
            Order order = orderClient.getOrder(ordenId);
            if (order != null) {
                for (String pid : order.getProductoIds()) {
                    try {
                        com.exam.broker.model.Product p = productClient.getProduct(pid);
                        if (p != null) {
                            itemsList.append("- ").append(p.getNombre()).append(": $").append(p.getPrecio()).append("\n");
                        }
                    } catch (Exception e) {
                        itemsList.append("- Producto ID: ").append(pid).append(" (Detalles no disponibles)\n");
                    }
                }
            }
        } catch (Exception e) {
            itemsList.append("(No se pudieron cargar los detalles de los productos)\n");
        }

        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");

            helper.setTo("dinocodeadvisor@gmail.com");
            helper.setSubject(subject);
            helper.setText(bodyText + "\n" + itemsList);

            mailSender.send(message);
            log.info("Status update email sent for order {}. Subject: {}", ordenId, subject);
        } catch (Exception e) {
            log.error("Failed to send status update email: {}", e.getMessage());
        }
    }
}
