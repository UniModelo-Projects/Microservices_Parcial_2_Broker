package com.exam.broker.client;

import com.exam.broker.model.Product;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

@FeignClient(name = "product-service")
public interface ProductClient {
    @PostMapping("/productos/retry")
    void retry(@RequestBody Product product);

    @GetMapping("/productos/{id}")
    Product getProduct(@PathVariable("id") String id);

    @PutMapping("/productos/{id}/stock/reduce")
    void reduceStock(@PathVariable("id") String id, @RequestParam("quantity") int quantity);
}
