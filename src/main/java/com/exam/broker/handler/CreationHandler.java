package com.exam.broker.handler;

import com.exam.broker.client.OrderClient;
import com.exam.broker.client.PaymentClient;
import com.exam.broker.client.ProductClient;
import com.exam.broker.model.RetryJob;
import com.exam.broker.repository.RetryJobRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.Map;

@Component
public class CreationHandler extends BaseHandler {

    @Autowired
    private ProductClient productClient;
    @Autowired
    private OrderClient orderClient;
    @Autowired
    private PaymentClient paymentClient;
    @Autowired
    private RetryJobRepository repository;

    @Autowired
    private org.springframework.kafka.core.KafkaTemplate<String, Object> kafkaTemplate;
    
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void handle(RetryJob job) throws Exception {
        if ("SUCCESS".equals(job.getStepAStatus())) {
            if (next != null) next.handle(job);
            return;
        }

        System.out.println("[Step A] Retrying creation for: " + job.getTopicType());
        java.util.Map<String, Object> fullPayload = objectMapper.readValue(job.getPayload(), new TypeReference<java.util.Map<String, Object>>() {});
        java.util.Map<String, Object> data = (java.util.Map<String, Object>) fullPayload.get("data");

        switch (job.getTopicType().toUpperCase()) {
            case "PRODUCTS":
            case "PRODUCT":
                com.exam.broker.model.Product product = objectMapper.convertValue(data, com.exam.broker.model.Product.class);
                productClient.retry(product);
                break;
            case "ORDERS":
            case "ORDER":
                com.exam.broker.model.Order order = objectMapper.convertValue(data, com.exam.broker.model.Order.class);
                orderClient.retry(order);
                break;
            case "PAYMENTS":
            case "PAYMENT":
                com.exam.broker.model.Payment payment = objectMapper.convertValue(data, com.exam.broker.model.Payment.class);
                com.exam.broker.model.Payment savedPayment = paymentClient.retry(payment);

                // NUEVO: Re-inyectar el evento en el flujo de negocio si es un pago exitoso
                if (savedPayment != null) {
                    System.out.println("[Step A] Re-injecting payment event for order: " + savedPayment.getOrdenId());
                    java.util.Map<String, Object> event = new java.util.HashMap<>();
                    event.put("ordenId", savedPayment.getOrdenId());
                    event.put("monto", savedPayment.getMonto());
                    event.put("pagoId", savedPayment.getId());
                    kafkaTemplate.send("payment_received_events", event);
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown TopicType: " + job.getTopicType());
        }

        job.setStepAStatus("SUCCESS");
        repository.save(job);

        if (next != null) next.handle(job);
    }
}
