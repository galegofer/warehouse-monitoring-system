package com.challenge.service;

import com.challenge.domain.Alarm;
import com.challenge.domain.Measurement;
import com.challenge.domain.SensorType;
import com.challenge.domain.ThresholdConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AlarmServiceTest {

    @Test
    void temp_36_threshold_35_shouldRaiseAlarm() {
        // given
        final var underTest = new AlarmService(new ThresholdConfig(35, 50));
        final var measurement =
                new Measurement("WH-1", "S-1", SensorType.TEMPERATURE, 36, 1_700_000_000L);

        // when
        final var result = underTest.evaluate(measurement);

        // then
        assertThat(result)
                .isPresent()
                .get()
                .returns(SensorType.TEMPERATURE, Alarm::type)
                .returns(36, Alarm::value)
                .returns(35, Alarm::thresholdUsed);
    }

    @Test
    void temp_35_threshold_35_shouldNotRaiseAlarm_becauseStrictlyGreater() {
        // given
        final var underTest = new AlarmService(new ThresholdConfig(35, 50));
        final var measurement =
                new Measurement("WH-1", "S-1", SensorType.TEMPERATURE, 35, 1_700_000_000L);

        // when
        final var result = underTest.evaluate(measurement);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void hum_51_threshold_50_shouldRaiseAlarm() {
        // given
        final var underTest = new AlarmService(new ThresholdConfig(35, 50));
        final var measurement =
                new Measurement("WH-2", "S-9", SensorType.HUMIDITY, 51, 1_700_000_100L);

        // when
        final var result = underTest.evaluate(measurement);

        // then
        assertThat(result)
                .isPresent()
                .get()
                .returns(SensorType.HUMIDITY, Alarm::type)
                .returns(51, Alarm::value)
                .returns(50, Alarm::thresholdUsed);
    }

    @Test
    void hum_50_threshold_50_shouldNotRaiseAlarm_becauseStrictlyGreater() {
        // given
        final var underTest = new AlarmService(new ThresholdConfig(35, 50));
        final var measurement =
                new Measurement("WH-2", "S-9", SensorType.HUMIDITY, 50, 1_700_000_100L);

        // when
        final var result = underTest.evaluate(measurement);

        // then
        assertThat(result).isEmpty();
    }
}
