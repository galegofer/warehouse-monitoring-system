package com.challenge.consumer;

import com.challenge.domain.SensorType;
import com.challenge.parser.MeasurementParser;
import com.challenge.publisher.JmsMeasurementPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

public class UdpMeasurementListener implements AutoCloseable {

    private static final int MAX_UDP_PAYLOAD = 512;
    private static final Logger logger = LoggerFactory.getLogger(UdpMeasurementListener.class);

    private final int port;
    private final MeasurementParser parser;
    private final JmsMeasurementPublisher publisher;
    private final SensorType sensorType;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private DatagramSocket socket;

    public UdpMeasurementListener(final int port, final MeasurementParser parser, SensorType sensorType, final JmsMeasurementPublisher publisher) {
        this.port = port;
        this.parser = parser;
        this.publisher = publisher;
        this.sensorType = sensorType;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;

        final var thread = new Thread(this::runLoop, "warehouse-udp-listener-" + sensorType.name().toLowerCase());
        thread.setDaemon(true);
        thread.start();
    }

    private void runLoop() {
        try (final var localSocket = new DatagramSocket(port)) {
            socket = localSocket;

            final var buf = new byte[2048];
            final var packet = new DatagramPacket(buf, buf.length);

            logger.info("UDP listener bound for {} sensor at port {}", sensorType.name().toLowerCase(), port);

            while (running.get()) {
                localSocket.receive(packet);

                final var payload = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8).trim();
                if (payload.isBlank()) continue;
                if (payload.length() > MAX_UDP_PAYLOAD) {
                    logger.warn("Ignoring oversized UDP payload size={} max={}", payload.length(), MAX_UDP_PAYLOAD);
                    continue;
                }

                try {
                    parser.parse(payload, sensorType)
                            .ifPresentOrElse(measurement -> {
                                logger.info("Received data for {} sensor at port {}", sensorType.name().toLowerCase(), port);
                                publisher.publish(measurement);
                            }, () -> logger.warn("Measurement not found or malformed."));
                } catch (final Exception ex) {
                    logger.warn("Invalid UDP payload='{}' error={}", payload, ex.toString());
                }
            }
        } catch (final Exception ex) {
            if (running.get()) {
                logger.error("UDP listener stopped unexpectedly: {}", ex.toString());
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        final var localSocket = socket;
        if (localSocket != null) {
            try {
                localSocket.close();
            } catch (final Exception ignored) {
            }
        }
    }
}
