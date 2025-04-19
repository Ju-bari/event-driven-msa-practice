package com.jh.runner.runnerImpl;


import com.jh.config.TwitterToKafkaServiceConfigData;
import com.jh.listener.TwitterKafkaStatusListener;
import com.jh.runner.StreamRunner;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import java.util.Arrays;

@Component
@ConditionalOnProperty(name="twitter-to-kafka-service.enable-mock-tweets", havingValue = "false", matchIfMissing = true)
@RequiredArgsConstructor
@Slf4j
public class TwitterKafkaStreamRunner implements StreamRunner {

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;
    private TwitterStream twitterStream;

    @Override
    public void start() throws Exception {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();
    }

    @PreDestroy
    public void shutdown() {
        if (twitterStream != null) {
            twitterStream.shutdown();
            log.info("Twitter stream shutdown.");
        }
    }

    private void addFilter() {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);

        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);

        log.info("Started filtering twitter stream for keywords: {}", Arrays.toString(keywords));
    }
}
