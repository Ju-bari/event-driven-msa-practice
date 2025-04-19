package com.jh;

import com.jh.init.StreamInitializer;
import com.jh.runner.StreamRunner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@RequiredArgsConstructor
@Slf4j
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    private final StreamInitializer streamInitializer;

    private final StreamRunner streamRunner;

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("App starts,,,");

        streamInitializer.init();
        streamRunner.start();
    }
}