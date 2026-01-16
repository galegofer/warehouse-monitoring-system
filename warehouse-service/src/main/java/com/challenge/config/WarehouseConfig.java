package com.challenge.config;

public record WarehouseConfig(
        String brokerUrl,
        String destinationName,
        int temperaturePort,
        int humidityPort,
        String warehouseId
) {
    public static WarehouseConfig load() {
        final var brokerUrl = readEnv("BROKER_URL", "tcp://localhost:61616");
        final var destinationName = readEnv("DESTINATION_NAME", "measurements.queue");
        final var udpTemperaturePort = readIntEnv("UDP_TEMPERATURE_PORT", 3344);
        final var udpHumidityPort = readIntEnv("UDP_HUMIDITY_PORT", 3355);
        final var warehouseId = readEnv("WAREHOUSE_ID", "WH-1");
        return new WarehouseConfig(brokerUrl, destinationName, udpTemperaturePort, udpHumidityPort, warehouseId);
    }

    private static String readEnv(final String name, final String defaultValue) {
        final var value = System.getenv(name);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    private static int readIntEnv(final String name, final int defaultValue) {
        final var value = System.getenv(name);
        if (value == null || value.isBlank()) return defaultValue;

        try {
            final var parsed = Integer.parseInt(value.trim());
            return parsed > 0 ? parsed : defaultValue;
        } catch (Exception ignored) {
            return defaultValue;
        }
    }
}
