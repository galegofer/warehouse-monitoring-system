package com.challenge;

import com.challenge.config.WarehouseConfig;
import com.challenge.consumer.UdpMeasurementListener;
import com.challenge.domain.SensorType;
import com.challenge.parser.MeasurementParser;
import com.challenge.publisher.JmsMeasurementPublisher;
import com.challenge.serialization.MeasurementJsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class WarehouseApplication {

    private static final Logger logger = LoggerFactory.getLogger(WarehouseApplication.class);

    public static void main(String[] args) {
        final var warehouseConfig = WarehouseConfig.load();

        final var mapper = new MeasurementJsonMapper();
        final var publisher = new JmsMeasurementPublisher(warehouseConfig.brokerUrl(), warehouseConfig.destinationName(), mapper);

        final var parser = new MeasurementParser(warehouseConfig.warehouseId());
        final var tempListener = new UdpMeasurementListener(warehouseConfig.temperaturePort(), parser, SensorType.TEMPERATURE, publisher);
        final var humListener = new UdpMeasurementListener(warehouseConfig.humidityPort(), parser, SensorType.HUMIDITY, publisher);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down warehouse-service");
            try {
                tempListener.close();
                humListener.close();
            } catch (Exception ignored) {
            }
            try {
                publisher.close();
            } catch (Exception ignored) {
            }
        }));

        logger.info("warehouse-service started. temperatureUdpPort={} humidityUdpPort={} warehouseId={} brokerUrl={} destination={}",
                warehouseConfig.temperaturePort(), warehouseConfig.humidityPort(), warehouseConfig.warehouseId(), warehouseConfig.brokerUrl(), warehouseConfig.destinationName());

        tempListener.start();
        humListener.start();
        keepAlive();
    }

    private static void keepAlive() {
        try {
            Thread.currentThread().join();
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
