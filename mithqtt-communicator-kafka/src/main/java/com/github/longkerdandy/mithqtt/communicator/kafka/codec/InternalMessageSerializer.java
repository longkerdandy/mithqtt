package com.github.longkerdandy.mithqtt.communicator.kafka.codec;

import com.github.longkerdandy.mithqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithqtt.communicator.kafka.util.JSONs;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

/**
 * Internal Message Serializer
 */
@SuppressWarnings("unused")
public class InternalMessageSerializer implements Serializer<InternalMessage> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to do
    }

    @Override
    public byte[] serialize(String topic, InternalMessage data) {
        try {
            return JSONs.Mapper.writeValueAsBytes(data);
        } catch (IOException e) {
            throw new SerializationException("Error when serializing internal message to byte[]", e);
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}
