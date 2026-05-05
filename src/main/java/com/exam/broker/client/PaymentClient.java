package com.exam.broker.client;

import com.exam.broker.model.Payment;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "payment-service")
public interface PaymentClient {
    @PostMapping("/pagos/retry")
    Payment retry(@RequestBody Payment payment);

    @org.springframework.web.bind.annotation.GetMapping("/pagos/orden/{ordenId}")
    java.util.List<Payment> getPaymentsByOrder(@org.springframework.web.bind.annotation.PathVariable("ordenId") String ordenId);
}
