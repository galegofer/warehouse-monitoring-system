package com.challenge.domain;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public record Measurement(@Nullable String warehouseId, @NotNull String sensorId, @NotNull SensorType type, int value,
                          long timestamp) {
}
