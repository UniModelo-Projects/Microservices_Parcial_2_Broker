package com.exam.broker.client;

import com.exam.broker.model.Product;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "product-service")
public interface ProductClient {
    @PostMapping("/productos/retry")
    void retry(@RequestBody Product product);

    @org.springframework.web.bind.annotation.PutMapping("/productos/{id}/stock/reduce")
    void reduceStock(@org.springframework.web.bind.annotation.PathVariable("id") String id, @org.springframework.web.bind.annotation.RequestParam("quantity") int quantity);
}
