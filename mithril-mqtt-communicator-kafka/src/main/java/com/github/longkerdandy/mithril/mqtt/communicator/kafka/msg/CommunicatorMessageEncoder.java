package com.github.longkerdandy.mithril.mqtt.communicator.kafka.msg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.longkerdandy.mithril.mqtt.api.comm.CommunicatorMessage;
import kafka.serializer.Encoder;

import static com.github.longkerdandy.mithril.mqtt.communicator.kafka.util.JSONs.Mapper;

/**
 * CommunicatorMessage Kafka Encoder
 */
public class CommunicatorMessageEncoder implements Encoder<CommunicatorMessage> {

    @Override
    public byte[] toBytes(CommunicatorMessage communicatorMessage) {
        try {
            return Mapper.writeValueAsBytes(communicatorMessage);
        } catch (JsonProcessingException e) {
            return new byte[0];
        }
    }
}
