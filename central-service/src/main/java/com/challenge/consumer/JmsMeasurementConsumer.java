package com.challenge.consumer;

import com.challenge.serialization.MeasurementJsonMapper;
import com.challenge.service.AlarmService;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

public class JmsMeasurementConsumer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(JmsMeasurementConsumer.class);

    private final String brokerUrl;
    private final String destinationName;
    private final AlarmService alarmService;
    private final MeasurementJsonMapper jsonMapper;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private Connection connection;
    private Session session;
    private MessageConsumer consumer;

    public JmsMeasurementConsumer(
            final String brokerUrl,
            final String destinationName,
            final AlarmService alarmService,
            final MeasurementJsonMapper jsonMapper
    ) {
        this.brokerUrl = brokerUrl;
        this.destinationName = destinationName;
        this.alarmService = alarmService;
        this.jsonMapper = jsonMapper;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;

        logger.info("Starting consumer...");
        final var thread = new Thread(this::runLoop, "central-jms-consumer");
        thread.setDaemon(true);
        thread.start();
    }

    private void runLoop() {
        final var reconnectDelay = Duration.ofSeconds(2);

        while (running.get()) {
            try {
                connect();
                consumeLoop();
            } catch (final Exception ex) {
                logger.warn("JMS consumer error, will reconnect: {}", ex.toString());
                safeCloseResources();
                sleep(reconnectDelay);
            }
        }

        safeCloseResources();
    }

    private void connect() throws JMSException {
        final var factory = new ActiveMQConnectionFactory(brokerUrl);

        connection = factory.createConnection();
        connection.start();

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final var destination = session.createQueue(destinationName);

        consumer = session.createConsumer(destination);

        logger.info("Connected to brokerUrl={} destination={}", brokerUrl, destinationName);
    }

    private void consumeLoop() throws JMSException {
        while (running.get()) {
            final var message = consumer.receive(1000);

            if (message == null) {
                continue;
            }

            if (!(message instanceof TextMessage textMessage)) {
                logger.warn("Ignoring non-text JMS message type={}", message.getClass().getName());
                continue;
            }

            final var payload = textMessage.getText();

            try {
                final var measurement = jsonMapper.fromJson(payload);
                alarmService.onMeasurement(measurement);
            } catch (final Exception ex) {
                logger.warn("Invalid message payload, ignoring. payload='{}' error={}", payload, ex.toString());
            }
        }
    }

    private static void sleep(final Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (final InterruptedException ignored) {
            logger.debug("Interrupted while waiting for messages to be consumed");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        running.set(false);
        safeCloseResources();
    }

    private void safeCloseResources() {
        try {
            if (consumer != null) consumer.close();
        } catch (final Exception ignored) {
        }
        try {
            if (session != null) session.close();
        } catch (final Exception ignored) {
        }
        try {
            if (connection != null) connection.close();
        } catch (final Exception ignored) {
        }

        consumer = null;
        session = null;
        connection = null;
    }
}
