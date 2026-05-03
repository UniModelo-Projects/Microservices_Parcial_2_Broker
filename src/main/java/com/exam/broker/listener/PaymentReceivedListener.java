package com.exam.broker.listener;

import com.exam.broker.client.OrderClient;
import com.exam.broker.client.PaymentClient;
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
import java.util.List;
import java.util.Map;

@Service
public class PaymentReceivedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentReceivedListener.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private OrderClient orderClient;

    @Autowired
    private PaymentClient paymentClient;

    @Autowired
    private EnvioRepository envioRepository;

    @Autowired
    private NotificationLogRepository notificationLogRepository;

    @Autowired
    private JavaMailSender mailSender;

    @KafkaListener(topics = "payment_received_events", groupId = "broker-payment-group")
    public void listen(String payload) {
        log.info("Received payment_received_events: {}", payload);
        try {
            Map<String, Object> event = objectMapper.readValue(payload, Map.class);
            String ordenId = (String) event.get("ordenId");
            String pagoId = (String) event.get("pagoId");

            // Idempotency check for this specific payment notification
            if (notificationLogRepository.findByOrdenIdAndTipoEvento(pagoId, "PAGO_PROCESADO").isPresent()) {
                log.info("Payment {} already processed, skipping", pagoId);
                return;
            }

            // Fetch order and all payments
            Order order = orderClient.getOrder(ordenId);
            if (order == null) {
                log.error("Order {} not found", ordenId);
                return;
            }

            List<Payment> payments = paymentClient.getPaymentsByOrder(ordenId);
            double totalPagado = payments.stream()
                    .filter(p -> "COMPLETADO".equals(p.getEstado()))
                    .mapToDouble(Payment::getMonto)
                    .sum();

            double saldoPendiente = order.getTotal() - totalPagado;

            // Send Email (Workflow Paso 2)
            sendPaymentEmail(order, totalPagado, saldoPendiente);

            // Decision: Order fully paid?
            if (saldoPendiente <= 0) {
                log.info("Order {} fully paid. Registering for shipping.", ordenId);
                orderClient.updateStatus(ordenId, "PAGADA");
                
                Envio envio = new Envio();
                envio.setOrdenId(ordenId);
                envio.setClienteEmail("dinocodeadvisor@gmail.com"); // Mock email or fetch from order if available
                envioRepository.save(envio);
            }

            // Log notification
            notificationLogRepository.save(new NotificationLog(pagoId, "PAGO_PROCESADO"));

        } catch (Exception e) {
            log.error("Error processing payment event: {}", e.getMessage());
        }
    }

    private void sendPaymentEmail(Order order, double totalPagado, double saldoPendiente) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo("dinocodeadvisor@gmail.com");
        
        if (saldoPendiente <= 0) {
            message.setSubject("✅ Pago Completado - Orden " + order.getId());
            message.setText("¡Gracias! Tu pago ha sido completado.\n\nTotal Orden: $" + order.getTotal() + "\nTu pedido ha pasado a logística.");
        } else {
            message.setSubject("💰 Pago Recibido (Parcial) - Orden " + order.getId());
            message.setText("Hemos recibido tu abono.\n\nTotal Orden: $" + order.getTotal() + 
                           "\nTotal Pagado: $" + totalPagado + 
                           "\nSaldo Pendiente: $" + saldoPendiente + 
                           "\n\nTu orden se procesará para envío una vez se cubra el total.");
        }
        
        try {
            mailSender.send(message);
            log.info("Payment email sent for order {}", order.getId());
        } catch (Exception e) {
            log.error("Failed to send payment email: {}", e.getMessage());
        }
    }
}
