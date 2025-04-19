package com.jh.listener;

import com.jh.config.KafkaConfigData;
import com.jh.service.KafkaProducer;
import com.jh.transformer.TwitterStatusToAvroTransformer;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
@RequiredArgsConstructor
@Slf4j
public class TwitterKafkaStatusListener extends StatusAdapter {

    private final KafkaConfigData kafkaConfigData;

    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;

    private final TwitterStatusToAvroTransformer twitterStatusToAvroTransformer;

    @Override
    public void onStatus(Status status) {
        log.info("Twitter status with text {}", status.getText());

        TwitterAvroModel twitterAvroModel = twitterStatusToAvroTransformer.getTwitterAvroModelFromStatus(status);
        kafkaProducer.send(kafkaConfigData.getTopicName(), twitterAvroModel.getUserId(), twitterAvroModel);
    }

}

