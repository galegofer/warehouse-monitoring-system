package com.challenge;

import com.challenge.config.ThresholdProvider;
import com.challenge.consumer.JmsMeasurementConsumer;
import com.challenge.serialization.MeasurementJsonMapper;
import com.challenge.service.AlarmService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CentralApplication {

    private static final Logger logger = LoggerFactory.getLogger(CentralApplication.class);

    public static void main(String[] args) {
        final var thresholds = ThresholdProvider.load();
        final var alarmService = new AlarmService(thresholds);
        final var mapper = new MeasurementJsonMapper();

        final var brokerUrl = readEnv("BROKER_URL", "tcp://activemq:61616");
        final var destinationName = readEnv("DESTINATION_NAME", "measurements.queue");

        @SuppressWarnings("resource") final var consumer = new JmsMeasurementConsumer(brokerUrl, destinationName, alarmService, mapper);
        consumer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down central-service");
            try {
                consumer.close();
            } catch (Exception ignored) {
            }
        }));

        logger.info("""
                       \s
                       _________           _____              ______   ________                  _____           \s
                        _  ____/_____________  /_____________ ___  /   __  ___/______________   ____(_)__________\s
                        _  /    _  _ \\_  __ \\  __/_  ___/  __ `/_  /    _____ \\_  _ \\_  ___/_ | / /_  /_  ___/  _ \\
                        / /___  /  __/  / / / /_ _  /   / /_/ /_  /     ____/ //  __/  /   __ |/ /_  / / /__ /  __/
                        \\____/  \\___//_/ /_/\\__/ /_/    \\__,_/ /_/      /____/ \\___//_/    _____/ /_/  \\___/ \\___/\s
                \s""");

        logger.info("central-service started. brokerUrl={} destination={}", brokerUrl, destinationName);

        keepAlive();
    }

    private static String readEnv(String name, String defaultValue) {
        final var value = System.getenv(name);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    private static void keepAlive() {
        try {
            Thread.currentThread().join();
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
