package com.challenge.serialization;


import com.challenge.domain.Measurement;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MeasurementJsonMapper {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public Measurement fromJson(String json) {
        try {
            return objectMapper.readValue(json, Measurement.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid measurement json", e);
        }
    }

    public String toJson(Measurement measurement) {
        try {
            return objectMapper.writeValueAsString(measurement);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize measurement", e);
        }
    }
}
