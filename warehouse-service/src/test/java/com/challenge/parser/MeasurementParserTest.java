package com.challenge.parser;

import com.challenge.domain.SensorType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MeasurementParserTest {

    private static final String DEFAULT_WAREHOUSE = "w1";

    private final MeasurementParser underTest = new MeasurementParser(DEFAULT_WAREHOUSE);

    @Test
    void parse_shouldParseValidPayload_temperature() {
        // given
        final var before = System.currentTimeMillis();
        final var payload = "sensor_id=t1; value=30";

        // when
        final var result = underTest.parse(payload, SensorType.TEMPERATURE);

        // then
        final var after = System.currentTimeMillis();
        assertTrue(result.isPresent());

        final var m = result.orElseThrow();
        assertEquals(DEFAULT_WAREHOUSE, m.warehouseId());
        assertEquals("t1", m.sensorId());
        assertEquals(SensorType.TEMPERATURE, m.type());
        assertEquals(30, m.value());
        assertTrue(m.timestamp() >= before && m.timestamp() <= after);
    }

    @Test
    void parse_shouldParseValidPayload_humidity() {
        // given
        final var payload = "sensor_id=h1; value=40";

        // when
        final var result = underTest.parse(payload, SensorType.HUMIDITY);

        // then
        assertTrue(result.isPresent());

        final var m = result.orElseThrow();
        assertEquals(DEFAULT_WAREHOUSE, m.warehouseId());
        assertEquals("h1", m.sensorId());
        assertEquals(SensorType.HUMIDITY, m.type());
        assertEquals(40, m.value());
        assertTrue(m.timestamp() > 0);
    }

    @Test
    void parse_shouldIgnoreOrderSpacesAndExtraTokens() {
        // given
        final var payload = "  value = 7 ; foo=bar ; sensor_id = s9  ";

        // when
        final var result = underTest.parse(payload, SensorType.TEMPERATURE);

        // then
        assertTrue(result.isPresent());
        assertEquals("s9", result.orElseThrow().sensorId());
        assertEquals(7, result.orElseThrow().value());
    }

    @Test
    void parse_shouldAllowNegativeNumbers() {
        // given
        final var payload = "sensor_id=t1; value=-10";

        // when
        final var result = underTest.parse(payload, SensorType.TEMPERATURE);

        // then
        assertTrue(result.isPresent());
        assertEquals(-10, result.orElseThrow().value());
    }

    @Test
    void parse_shouldUseLastValue_whenDuplicateKeys() {
        // given
        final var payload = "sensor_id=t1; value=10; value=20";

        // when
        final var result = underTest.parse(payload, SensorType.TEMPERATURE);

        // then
        assertTrue(result.isPresent());
        assertEquals(20, result.orElseThrow().value());
    }

    @Test
    void parse_shouldUseLastSensorId_whenDuplicateKeys() {
        // given
        final var payload = "sensor_id=old; sensor_id=new; value=1";

        // when
        final var result = underTest.parse(payload, SensorType.TEMPERATURE);

        // then
        assertTrue(result.isPresent());
        assertEquals("new", result.orElseThrow().sensorId());
    }

    @ParameterizedTest(name = "[{index}] payload=\"{0}\", type={1}")
    @MethodSource("invalidInputs")
    void parse_shouldReturnEmpty_forInvalidInputs(final String payload, final SensorType type) {
        // given

        // when
        final var result = underTest.parse(payload, type);

        // then
        assertTrue(result.isEmpty());
    }

    private static Stream<Arguments> invalidInputs() {
        return Stream.of(
                Arguments.of(null, SensorType.TEMPERATURE),
                Arguments.of("   ", SensorType.TEMPERATURE),
                Arguments.of("sensor_id=t1; value=30", null),

                Arguments.of("value=30", SensorType.TEMPERATURE),
                Arguments.of("sensor_id=t1", SensorType.TEMPERATURE),
                Arguments.of("sensor_id=   ; value=30", SensorType.TEMPERATURE),
                Arguments.of("sensor_id=t1; value=   ", SensorType.TEMPERATURE),

                Arguments.of("sensor=t1; val=30", SensorType.TEMPERATURE),
                Arguments.of("sensor_id t1; value 30", SensorType.TEMPERATURE),

                Arguments.of("=t1; value=30", SensorType.TEMPERATURE),
                Arguments.of("sensor_id=; value=30", SensorType.TEMPERATURE),

                Arguments.of("sensor_id=t1; value=abc", SensorType.TEMPERATURE),
                Arguments.of("sensor_id=t1; value=30.5", SensorType.TEMPERATURE),
                Arguments.of("sensor_id=t1; value=2147483648", SensorType.TEMPERATURE),

                Arguments.of("sensor_id=t1; value=12=34", SensorType.TEMPERATURE)
        );
    }
}
