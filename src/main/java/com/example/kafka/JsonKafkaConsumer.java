package com.example.kafka;

import com.example.payload.User;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class JsonKafkaConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonKafkaConsumer.class);

    @KafkaListener(topics = "topic_demo_json", groupId = "myGroup")
    public void consumeJsonMessage(ConsumerRecord<String, User> record) {
        // Lấy thông tin từ tin nhắn
        String key = record.key();
        User user = record.value();
        int partition = record.partition();
        long offset = record.offset();

        // Xử lý tin nhắn
        LOGGER.info("Received JSON message - Key: {}, User: {}, Partition: {}, Offset: {}", key, user, partition, offset);

        // Xử lý logic dựa trên đối tượng User
        // ...
    }
}
