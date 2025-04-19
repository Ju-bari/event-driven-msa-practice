package com.jh.service.Impl;

import com.jh.service.KafkaProducer;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

    private KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        log.info("Sending message to Kafka. Topic: {}, Key: {}, Message: {}", topicName, key, message);

        CompletableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture =
                kafkaTemplate.send(topicName, key, message);

        kafkaResultFuture.whenComplete((result, throwable) -> {
            if (throwable != null) {
                log.error("Failed to send message to Kafka. Topic: {}, Key: {}, Message: {}, Error: {}",
                        topicName, key, message, throwable.getMessage(), throwable);
            } else {
                RecordMetadata metadata = result.getRecordMetadata();
                log.info("Message sent successfully. Topic: {}, Partition: {}, Offset: {}, Timestamp: {}, SentAt: {}",
                        metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp(), Instant.now());
            }
        });
    }

    @PreDestroy
    public void close() {
        if (kafkaTemplate != null) {
            log.info("Closing kafka producer");
            kafkaTemplate.destroy();
        }
    }

}
