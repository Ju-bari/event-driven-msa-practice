package com.jh.client;

import com.jh.config.KafkaConfigData;
import com.jh.config.RetryConfigData;
import com.jh.exception.KafkaClientException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaAdminClient {

    private final KafkaConfigData kafkaConfigData;

    private final RetryConfigData retryConfigData;

    private final AdminClient adminClient;

    private final RetryTemplate retryTemplate;

    private final WebClient webClient;

    // retry
    public void createTopics() {
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopics); // RetryTemplate 사용
            log.info("create topics result: {}", createTopicsResult.values());
        } catch (Throwable t) {
            throw new KafkaClientException("Reached max number of retry for creating kafka topic!", t);
        }

        checkTopicsCreated();
    }

    /**
     * Kafka는 토픽 생성 요청을 비동기적으로 처리하기 때문에,
     * createTopics() 호출 직후에는 토픽이 아직 존재하지 않을 수 있다.
     * <p>
     * 이 메서드는 설정된 토픽들이 실제로 Kafka 클러스터에 생성될 때까지
     * 폴링 방식으로 확인하고, 생성이 완료되지 않으면 재시도한다.
     * <p>
     * 재시도는 설정된 최대 횟수까지 수행되며,
     * 각 재시도 사이에는 점점 증가하는 대기 시간(지수 백오프)을 둔다.
     */
    public void checkTopicsCreated() {
        Collection<TopicListing> topics = getTopics();
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        int multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();

        for (String topic : kafkaConfigData.getTopicNamesToCreate()) {
            while (!isTopicCreate(topics, topic)) {
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;
                topics = getTopics();
            }
        }
    }

    // 수동 retry
    public void checkSchemaRegistry() {
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        int multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();

        while (!getSchemaRegistryStatus().is2xxSuccessful()) {
            checkMaxRetry(retryCount++, maxRetry);
            sleep(sleepTimeMs);
            sleepTimeMs *= multiplier;
        }
    }

    private HttpStatus getSchemaRegistryStatus() {
        try {
            return webClient
                    .method(HttpMethod.GET)
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .retrieve()
                    .toBodilessEntity()
                    .map(entity -> HttpStatus.valueOf(entity.getStatusCode().value()))
                    .block();
        } catch (Exception e) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }

    private boolean isTopicCreate(Collection<TopicListing> topics, String topicName) {
        if (topics == null) {
            return false;
        }
        return topics.stream().anyMatch(topic -> topic.name().equals(topicName));
    }

    private void checkMaxRetry(Integer retry, Integer maxRetry) {
        if (retry > maxRetry) {
            throw new KafkaClientException("Reached max number of retry for reading kafka topics!");
        }
    }

    private void sleep(Long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new KafkaClientException("Error while sleeping for waiting new created topics!");
        }
    }

    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
        log.info("Create {} topic, attempt {}", topicNames.size(), retryContext.getRetryCount());

        List<NewTopic> kafkaTopics = topicNames.stream().map(topic -> new NewTopic(
                topic.trim(),
                kafkaConfigData.getNumOfPartitions(),
                kafkaConfigData.getReplicationFactor()
        )).collect(Collectors.toList());

        return adminClient.createTopics(kafkaTopics);
    }

    // retry
    private Collection<TopicListing> getTopics() {
        Collection<TopicListing> topics;

        try {
            topics = retryTemplate.execute(this::doGetTopics);
        } catch (Throwable t) {
            throw new KafkaClientException("Reached max number of retry fro creating kafka topic!", t);
        }

        return topics;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext) throws ExecutionException, InterruptedException {
        log.info("Reading kafka topic {}, attempt {}",
                kafkaConfigData.getTopicNamesToCreate().toArray(), retryContext.getRetryCount());

        Collection<TopicListing> topics = adminClient.listTopics().listings().get();

        if (topics != null) {
            topics.forEach(topic -> {
                log.debug("Topic with name {}", topic.name());
            });
        }

        return topics;
    }

}
