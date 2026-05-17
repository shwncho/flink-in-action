package com.example.demo.kafka;

import com.example.demo.flink.domain.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class MessageController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.input-topic}")
    private String topic;

    @PostMapping
    public Order publish(@RequestBody Order order) throws JsonProcessingException {
        String payload = objectMapper.writeValueAsString(order);
        kafkaTemplate.send(topic, order.orderId(), payload);
        return order;
    }
}
