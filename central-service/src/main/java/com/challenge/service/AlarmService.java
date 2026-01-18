package com.challenge.service;

import com.challenge.domain.Alarm;
import com.challenge.domain.Measurement;
import com.challenge.domain.ThresholdConfig;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

public class AlarmService {

    private static final Logger logger = LoggerFactory.getLogger(AlarmService.class);

    private final ThresholdConfig thresholdConfig;
    private final Cache<String, Boolean> messageDedup = Caffeine.newBuilder()
            .maximumSize(50_000)
            .expireAfterWrite(Duration.ofSeconds(15))
            .build();

    public AlarmService(ThresholdConfig thresholdConfig) {
        this.thresholdConfig = thresholdConfig;
    }

    public void onMeasurement(@NotNull final Measurement measurement) {
        evaluate(measurement)
                .ifPresent(alarm -> {
                            final var key = alarm.warehouseId() + "|" + alarm.sensorId() + "|" + alarm.type();

                            if (messageDedup.asMap().putIfAbsent(key, Boolean.TRUE) != null) {
                                logger.warn("Message with key {} already exists and will be ignored", key);
                                return;
                            }

                            logger.warn("ALARM warehouse={} sensor={} type={} value={} threshold={} ts={}", alarm.warehouseId(), alarm.sensorId(), alarm.type(), alarm.value(), alarm.thresholdUsed(), alarm.timestamp());
                        }
                );
    }

    Optional<Alarm> evaluate(@NotNull final Measurement measurement) {
        switch (measurement.type()) {
            case TEMPERATURE -> {
                return measurement.value() > thresholdConfig.temperature()
                        ? Optional.of(new Alarm(measurement.warehouseId(), measurement.sensorId(), measurement.type(), measurement.value(), thresholdConfig.temperature(), measurement.timestamp()))
                        : Optional.empty();
            }
            case HUMIDITY -> {
                return measurement.value() > thresholdConfig.humidity()
                        ? Optional.of(new Alarm(measurement.warehouseId(), measurement.sensorId(), measurement.type(), measurement.value(), thresholdConfig.humidity(), measurement.timestamp()))
                        : Optional.empty();

            }
            default -> {
                logger.warn("Unknown measurement type={}", measurement.type());
                return Optional.empty();
            }
        }
    }
}
