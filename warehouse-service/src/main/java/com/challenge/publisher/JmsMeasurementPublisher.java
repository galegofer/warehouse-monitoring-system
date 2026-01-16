package com.challenge.publisher;

import com.challenge.domain.Measurement;
import com.challenge.serialization.MeasurementJsonMapper;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class JmsMeasurementPublisher implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(JmsMeasurementPublisher.class);

    private final String brokerUrl;
    private final String destinationName;
    private final MeasurementJsonMapper mapper;

    private Connection connection;
    private Session session;
    private MessageProducer producer;

    public JmsMeasurementPublisher(final String brokerUrl, final String destinationName, final MeasurementJsonMapper mapper) {
        this.brokerUrl = brokerUrl;
        this.destinationName = destinationName;
        this.mapper = mapper;
        connect();
    }

    public synchronized void publish(final Measurement measurement) {
        final var json = mapper.toJson(measurement);

        try {
            ensureConnected();
            producer.send(session.createTextMessage(json));
        } catch (final Exception ex) {
            logger.warn("Publish failed, will reconnect. error={}", ex.toString());
            safeClose();
            connect();

            try {
                ensureConnected();
                producer.send(session.createTextMessage(json));
            } catch (final Exception ex2) {
                logger.warn("Publish failed after reconnect, dropping message. error={}", ex2.toString());
            }
        }
    }

    private void connect() {
        try {
            final ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);

            connection = factory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Destination destination = session.createQueue(destinationName);

            producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            logger.info("Publisher connected. brokerUrl={} destination={}", brokerUrl, destinationName);
        } catch (final Exception ex) {
            logger.warn("Publisher failed to connect (will retry on publish). error={}", ex.toString());
            safeClose();
        }
    }

    private void ensureConnected() throws JMSException {
        if (connection == null || session == null || producer == null) {
            throw new JMSException("Publisher not connected");
        }
    }

    @Override
    public synchronized void close() {
        safeClose();
    }

    private void safeClose() {
        try {
            if (producer != null) producer.close();
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
        producer = null;
        session = null;
        connection = null;
    }
}
