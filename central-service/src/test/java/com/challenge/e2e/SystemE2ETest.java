package com.challenge.e2e;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class SystemE2ETest {

    private static final int TEMP_PORT = 3344;
    private static final int HUM_PORT = 3355;

    private static final StringBuilder centralLogs = new StringBuilder();

    @Container
    static final ComposeContainer environment =
            new ComposeContainer(composeFileFromRepoRoot().toFile())
                    .withLocalCompose(true)
                    .waitingFor("activemq", Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)))
                    .waitingFor("central-service",
                            Wait.forLogMessage(".*central-service started.*", 1)
                                    .withStartupTimeout(Duration.ofMinutes(2)))
                    .waitingFor("warehouse-service",
                            Wait.forLogMessage(".*warehouse-service started.*", 1)
                                    .withStartupTimeout(Duration.ofMinutes(2)))
                    .withLogConsumer("central-service", frame -> centralLogs.append(frame.getUtf8String()));


    @BeforeEach
    void resetCapturedCentralLogs() {
        centralLogs.setLength(0);
    }

    @Test
    void temperature_40_shouldTriggerAlarmInCentralLogs() throws IOException, InterruptedException {
        sendUdpInsideDockerNetwork(TEMP_PORT, "sensor_id=t1; value=40");

        awaitCentralLogContains(allOf(
                "ALARM",
                "type=TEMPERATURE",
                "sensor=t1"
        ));
    }

    @Test
    void temperature_40_shouldTriggerDedupInCentralLogs() throws IOException, InterruptedException {
        sendUdpInsideDockerNetwork(TEMP_PORT, "sensor_id=t1; value=40");

        awaitCentralLogContains(allOf(
                "Message with key WH-1|t1|TEMPERATURE already exists and will be ignored"
        ));
    }

    @Test
    void humidity_51_shouldTriggerAlarmInCentralLogs() throws IOException, InterruptedException {
        sendUdpInsideDockerNetwork(HUM_PORT, "sensor_id=h1; value=51");

        awaitCentralLogContains(allOf(
                "ALARM",
                "type=HUMIDITY",
                "sensor=h1"
        ));
    }


    @Test
    void malformedButRecoverablePayload_shouldTriggerAlarm() throws IOException, InterruptedException {
        sendUdpInsideDockerNetwork(TEMP_PORT, "  sensor_id = t3 ; value = 10 ; sensor_id=t3 ; value=40  ");

        awaitCentralLogContains(allOf(
                "ALARM",
                "type=TEMPERATURE",
                "sensor=t3"
        ));
    }

    private static void sendUdpInsideDockerNetwork(final int port, final String payload) throws IOException, InterruptedException {
        final var sender = environment.getContainerByServiceName("udp-sender-1")
                .orElseThrow(() -> new IllegalStateException("udp-sender container not found"));

        sender.execInContainer("sh", "-c", "apk add --no-cache socat >/dev/null && " + "echo -n '" + payload.replace("'", "'\\''") + "' | " + "socat - UDP:warehouse-service:" + port);
    }

    private static void awaitCentralLogContains(final Pattern pattern) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> assertThat(centralLogs.toString()).containsPattern(pattern));
    }

    private static Pattern allOf(final String... parts) {
        final var lookaheads = Stream.of(parts)
                .map(Pattern::quote)
                .map(q -> "(?=.*" + q + ")")
                .reduce("", String::concat);

        return Pattern.compile("(?s)" + lookaheads + ".*");
    }

    private static Path composeFileFromRepoRoot() {
        return Path.of(System.getProperty("user.dir"))
                .toAbsolutePath()
                .normalize()
                .getParent()
                .resolve("docker-compose.yaml");
    }
}
