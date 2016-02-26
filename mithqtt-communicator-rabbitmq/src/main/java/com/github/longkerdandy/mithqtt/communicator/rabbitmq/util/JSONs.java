package com.github.longkerdandy.mithqtt.communicator.rabbitmq.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.*;
import com.github.longkerdandy.mithqtt.api.internal.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * JSON Utils
 */
public class JSONs {

    private static final Logger logger = LoggerFactory.getLogger(JSONs.class);

    // Global JSON ObjectMapper
    public static final ObjectMapper Mapper = new ObjectMapper();

    static {
        Mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        Mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        Mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    /**
     * Decode bytes (json data) to InternalMessage
     */
    @SuppressWarnings("unchecked")
    public static InternalMessage decodeInternalMessage(byte[] data) {
        try {
            JavaType type = Mapper.getTypeFactory().constructParametrizedType(InternalMessage.class, InternalMessage.class, JsonNode.class);
            InternalMessage m = Mapper.readValue(data, type);
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
                    logger.warn("Error when deserializing byte[] to internal message due to unknown message type {}", m.getMessageType());
                    return null;
            }
        } catch (IOException e) {
            logger.warn("Error when deserializing byte[] to internal message due to json parse error", e);
            return null;
        }
    }

    private JSONs() {
    }
}
