package com.challenge.parser;

import com.challenge.domain.Measurement;
import com.challenge.domain.SensorType;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MeasurementParser {

    private final String defaultWarehouseId;

    private static final Logger logger = LoggerFactory.getLogger(MeasurementParser.class);
    private static final Pattern MEASUREMENT_PATTERN = Pattern.compile(
            "^\\s*sensor_id\\s*=\\s*([a-zA-Z0-9]+)\\s*;\\s*value\\s*=\\s*(\\d+)\\s*$"
    );

    public MeasurementParser(@Nullable final String defaultWarehouseId) {
        this.defaultWarehouseId = Optional.ofNullable(defaultWarehouseId)
                .orElseGet(() -> UUID.randomUUID().toString());
    }

    public Optional<Measurement> parse(@NotNull final String payload, @Nullable final SensorType type) {
        if (StringUtils.isBlank(payload) || type == null) return Optional.empty();

        if(!MEASUREMENT_PATTERN.matcher(payload).matches()) {
            logger.warn(
                    "Received payload '{}' does not match expected format (sensor_id=<alphanumeric>; value=<integer>), attempting tolerant parsing",
                    payload
            );
        }

        final var kv = parseKeyValue(payload);

        final var sensorId = StringUtils.trimToNull(kv.get("sensor_id"));
        final var valueRaw = StringUtils.trimToNull(kv.get("value"));

        if (sensorId == null || valueRaw == null) return Optional.empty();

        try {
            final int value = Integer.parseInt(valueRaw);

            final var timestamp = System.currentTimeMillis();
            return Optional.of(new Measurement(defaultWarehouseId, sensorId, type, value, timestamp));
        } catch (final Exception ignored) {
            return Optional.empty();
        }
    }

    private static Map<String, String> parseKeyValue(final @NotNull String payload) {
        return Arrays.stream(payload.split(";"))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .map(p -> p.split("=", 2))
                .filter(arr -> arr.length == 2)
                .collect(Collectors.toMap(
                        arr -> StringUtils.trimToEmpty(arr[0]),
                        arr -> StringUtils.trimToEmpty(arr[1]),
                        (a, b) -> b
                ));
    }
}
