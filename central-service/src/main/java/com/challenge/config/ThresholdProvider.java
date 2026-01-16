package com.challenge.config;

import com.challenge.domain.ThresholdConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThresholdProvider {

    private static final Logger logger = LoggerFactory.getLogger(ThresholdProvider.class);

    public static ThresholdConfig load() {
        final var temperatureThreshold =
                readThreshold("temperature.threshold", "TEMPERATURE_THRESHOLD", 35);
        final var humidityThreshold =
                readThreshold("humidity.threshold", "HUMIDITY_THRESHOLD", 50);

        logger.info("Loaded thresholds: temperature={}, humidity={}", temperatureThreshold, humidityThreshold);
        return new ThresholdConfig(temperatureThreshold, humidityThreshold);
    }

    private static int readThreshold(final String propertyName, final String envName, final int defaultValue) {
        final var raw = readRaw(propertyName, envName);

        if (StringUtils.isBlank(raw)) {
            logger.info(
                    "Missing threshold (property={}, env={}); using default {}",
                    propertyName,
                    envName,
                    defaultValue
            );
            return defaultValue;
        }

        final var normalized = raw.trim();

        try {
            final var value = Integer.parseInt(normalized);

            if (value < 0) {
                logger.warn(
                        "Negative threshold '{}' (property={}, env={}); using default {}",
                        normalized,
                        propertyName,
                        envName,
                        defaultValue
                );
                return defaultValue;
            }

            return value;
        } catch (final NumberFormatException ex) {
            logger.warn(
                    "Invalid threshold '{}' (property={}, env={}); using default {}",
                    normalized,
                    propertyName,
                    envName,
                    defaultValue
            );
            return defaultValue;
        }
    }

    private static String readRaw(final String propertyName, final String envName) {
        var raw = System.getProperty(propertyName);
        if (StringUtils.isBlank(raw)) {
            raw = System.getenv(envName);
        }
        return raw;
    }
}
