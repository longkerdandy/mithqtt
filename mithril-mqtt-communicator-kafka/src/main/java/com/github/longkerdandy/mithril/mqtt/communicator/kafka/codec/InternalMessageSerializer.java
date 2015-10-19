package com.github.longkerdandy.mithril.mqtt.communicator.kafka.codec;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

import static com.github.longkerdandy.mithril.mqtt.communicator.kafka.util.JSONs.Mapper;

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
            return Mapper.writeValueAsBytes(data);
        } catch (IOException e) {
            throw new SerializationException("Error when serializing internal message to byte[]", e);
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}
