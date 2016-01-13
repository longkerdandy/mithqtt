package com.github.longkerdandy.mithqtt.communicator.kafka.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * JSON Utils
 */
public class JSONs {

    // Global JSON ObjectMapper
    public static final ObjectMapper Mapper = new ObjectMapper();

    static {
        Mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        Mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        Mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    private JSONs() {
    }
}
