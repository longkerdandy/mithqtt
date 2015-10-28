package com.github.longkerdandy.mithril.mqtt.communicator.kafka.codec;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.longkerdandy.mithril.mqtt.api.internal.*;
import kafka.serializer.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.github.longkerdandy.mithril.mqtt.communicator.kafka.util.JSONs.Mapper;

/**
 * Internal Message Kafka Decoder
 */
@SuppressWarnings("unused")
public class InternalMessageDecoder implements Decoder<InternalMessage> {

    private static final Logger logger = LoggerFactory.getLogger(InternalMessageDecoder.class);

    @Override
    @SuppressWarnings("unchecked")
    public InternalMessage fromBytes(byte[] bytes) {
        try {
            JavaType type = Mapper.getTypeFactory().constructParametrizedType(InternalMessage.class, InternalMessage.class, JsonNode.class);
            InternalMessage m = Mapper.readValue(bytes, type);
            switch (m.getMessageType()) {
                case CONNECT:
                    Connect connect = Mapper.treeToValue((JsonNode) m.getPayload(), Connect.class);
                    m.setPayload(connect);
                    return m;
                case CONNACK:
                    ConnAck connack = Mapper.treeToValue((JsonNode) m.getPayload(), ConnAck.class);
                    m.setPayload(connack);
                    return m;
                case SUBSCRIBE:
                    Subscribe subscribe = Mapper.treeToValue((JsonNode) m.getPayload(), Subscribe.class);
                    m.setPayload(subscribe);
                    return m;
                case SUBACK:
                    SubAck suback = Mapper.treeToValue((JsonNode) m.getPayload(), SubAck.class);
                    m.setPayload(suback);
                    return m;
                case UNSUBSCRIBE:
                    Unsubscribe unsubscribe = Mapper.treeToValue((JsonNode) m.getPayload(), Unsubscribe.class);
                    m.setPayload(unsubscribe);
                    return m;
                case PUBLISH:
                    Publish publish = Mapper.treeToValue((JsonNode) m.getPayload(), Publish.class);
                    m.setPayload(publish);
                    return m;
                case UNSUBACK:
                case PUBACK:
                case PUBREC:
                case PUBREL:
                case PUBCOMP:
                    PacketId packetId = Mapper.treeToValue((JsonNode) m.getPayload(), PacketId.class);
                    m.setPayload(packetId);
                    return m;
                case PINGREQ:
                case PINGRESP:
                case DISCONNECT:
                    return m;
                default:
                    logger.warn("Decode error: Unknown internal message type: {}", m.getMessageType());
                    return null;
            }
        } catch (IOException e) {
            logger.warn("Decode error: Decode internal message with error: ", e);
            return null;
        }
    }
}
