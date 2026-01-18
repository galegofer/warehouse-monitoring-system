package com.challenge.consumer;

import com.challenge.domain.Measurement;
import com.challenge.serialization.MeasurementJsonMapper;
import com.challenge.service.AlarmService;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class JmsMeasurementConsumer implements AutoCloseable {

    private static final int MAX_PAYLOAD_SIZE = 10 * 1024;
    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(2);

    private static final Logger logger = LoggerFactory.getLogger(JmsMeasurementConsumer.class);

    private final String brokerUrl;
    private final String destinationName;
    private final AlarmService alarmService;
    private final MeasurementJsonMapper jsonMapper;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "jms-reconnector"));

    private Connection connection;
    private Session session;
    private MessageConsumer consumer;

    public JmsMeasurementConsumer(
            @NotNull final String brokerUrl,
            @NotNull final String destinationName,
            @NotNull final AlarmService alarmService,
            @NotNull final MeasurementJsonMapper jsonMapper
    ) {
        this.brokerUrl = brokerUrl;
        this.destinationName = destinationName;
        this.alarmService = alarmService;
        this.jsonMapper = jsonMapper;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        connectWithRetry();
    }

    private void connectWithRetry() {
        try {
            connect();
        } catch (final Exception ex) {
            logger.warn("JMS connection failed, retrying in {}s: {}",
                    RECONNECT_DELAY.toSeconds(), ex.toString());

            scheduleReconnect();
        }
    }

    private void connect() throws JMSException {
        final var factory = new ActiveMQConnectionFactory(brokerUrl);

        connection = factory.createConnection();
        connection.setExceptionListener(this::onJmsException);

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final var destination = session.createQueue(destinationName);
        consumer = session.createConsumer(destination);
        consumer.setMessageListener(this::onMessage);

        connection.start();

        logger.info("Connected to JMS brokerUrl={} destination={}", brokerUrl, destinationName);
    }

    private void onJmsException(final JMSException ex) {
        if (!running.get()) return;

        logger.warn("JMS exception detected, reconnecting: {}", ex.toString());

        safeCloseResources();
        scheduleReconnect();
    }

    private void scheduleReconnect() {
        if (!running.get()) return;

        scheduler.schedule(
                this::connectWithRetry,
                RECONNECT_DELAY.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    private void onMessage(final Message message) {
        if (!running.get()) return;

        if (!(message instanceof final TextMessage textMessage)) {
            logger.warn("Ignoring non-text JMS message type={}", message.getClass().getName());
            return;
        }

        try {
            final var payload = textMessage.getText();

            if (StringUtils.isBlank(payload)) {
                logger.warn("Ignoring empty JMS message");
                return;
            }

            if (payload.length() > MAX_PAYLOAD_SIZE) {
                logger.warn("Ignoring oversized JMS message size={} max={}",
                        payload.length(), MAX_PAYLOAD_SIZE);
                return;
            }

            processPayload(payload);
        } catch (final Exception ex) {
            logger.warn("Invalid message payload, ignoring. error={}", ex.toString());
        }
    }

    private void processPayload(final String payload) {
        try {
            final var measurement = jsonMapper.fromJson(payload);

            if (!isValid(measurement)) {
                logger.warn("Ignoring invalid Measurement payload={}", payload);
                return;
            }

            alarmService.onMeasurement(measurement);
        } catch (final Exception ex) {
            logger.warn(
                    "Invalid message payload, ignoring. payload='{}' error={}",
                    payload, ex.toString()
            );
        }
    }

    private static boolean isValid(final Measurement m) {
        return m != null
                && m.sensorId() != null
                && !m.sensorId().isBlank()
                && m.type() != null
                && m.timestamp() > 0;
    }

    @Override
    public void close() {
        running.set(false);

        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Scheduler did not terminate in time, forcing shutdown");
                scheduler.shutdownNow();
            }
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }

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
