package com.jh.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "twitter-to-kafka-service")
@Data
public class TwitterToKafkaServiceConfigData {
    private List<String> twitterKeywords;
    private String welcomeMessage;
    private Boolean enableMockTweets;
    private Integer mockMinTweetLength;
    private Integer mockMaxTweetLength;
    private Long mockSleepMs;
}
