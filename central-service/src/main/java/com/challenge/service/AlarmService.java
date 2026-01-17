package com.challenge.service;

import com.challenge.domain.Measurement;
import com.challenge.domain.SensorType;
import com.challenge.domain.ThresholdConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class AlarmService {

    private static final Logger logger = LoggerFactory.getLogger(AlarmService.class);

    private final ThresholdConfig thresholdConfig;

    public AlarmService(ThresholdConfig thresholdConfig) {
        this.thresholdConfig = thresholdConfig;
    }

    public void onMeasurement(@NotNull final Measurement measurement) {
        evaluate(measurement)
                .ifPresent(alarm ->
                        logger.warn("ALARM warehouse={} sensor={} type={} value={} threshold={} ts={}", alarm.warehouseId(), alarm.sensorId(), alarm.type(), alarm.value(), alarm.thresholdUsed(), alarm.timestamp())
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

    record Alarm(String warehouseId, String sensorId, SensorType type, int value, int thresholdUsed, long timestamp) {
    }
}
