package com.github.longkerdandy.mithqtt.storage.redis.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;


/**
 * JSON Utils
 */
public class JSONs {

    // Global JSON ObjectMapper
    public static final ObjectMapper ObjectMapper = new ObjectMapper();

    static {
        ObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        ObjectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        ObjectMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        ObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    private JSONs() {
    }
}
