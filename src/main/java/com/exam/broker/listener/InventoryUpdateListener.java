package com.exam.broker.listener;

import com.exam.broker.client.ProductClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;

@Service
public class InventoryUpdateListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryUpdateListener.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private ProductClient productClient;

    @KafkaListener(topics = "inventory_update_events", groupId = "broker-inventory-group")
    public void listen(String payload) {
        log.info("Received inventory_update_events: {}", payload);
        try {
            Map<String, Object> event = objectMapper.readValue(payload, Map.class);
            List<String> productoIds = (List<String>) event.get("productoIds");
            String accion = (String) event.get("accion");

            if ("REDUCIR_STOCK".equals(accion)) {
                for (String productId : productoIds) {
                    try {
                        log.info("Requesting stock reduction for product {}", productId);
                        productClient.reduceStock(productId, 1); // Assuming 1 unit per order entry for now
                    } catch (Exception e) {
                        log.error("Failed to reduce stock for product {}: {}", productId, e.getMessage());
                        // In a real scenario, this might trigger a compensation or a retry job
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error processing inventory update event: {}", e.getMessage());
        }
    }
}
