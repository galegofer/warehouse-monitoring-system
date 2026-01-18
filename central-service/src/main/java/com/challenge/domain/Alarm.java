package com.challenge.domain;

public record Alarm(String warehouseId, String sensorId, SensorType type, int value, int thresholdUsed, long timestamp) {
}
