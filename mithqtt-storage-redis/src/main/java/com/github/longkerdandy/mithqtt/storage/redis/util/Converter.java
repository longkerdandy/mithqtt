package com.github.longkerdandy.mithqtt.storage.redis.util;

import com.github.longkerdandy.mithqtt.api.message.Message;
import com.github.longkerdandy.mithqtt.api.message.MqttAdditionalHeader;
import com.github.longkerdandy.mithqtt.api.message.MqttPublishPayload;
import io.netty.handler.codec.mqtt.*;
import org.apache.commons.lang3.BooleanUtils;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 * Converter Utils
 */
public class Converter {

    /**
     * Convert Map to (MQTT) Message
     *
     * @param map Map
     * @return (MQTT) Message
     */
    public static Message mapToMessage(Map<String, String> map) {
        if (map == null || map.isEmpty()) return null;

        int type = Integer.parseInt(map.get("type"));
        if (type == MqttMessageType.PUBLISH.value()) {
            byte[] bytes = null;
            if (map.get("payload") != null) try {
                bytes = map.get("payload").getBytes("ISO-8859-1");
            } catch (UnsupportedEncodingException ignore) {
            }
            return new Message<>(
                    new MqttFixedHeader(
                            MqttMessageType.PUBLISH,
                            BooleanUtils.toBoolean(map.getOrDefault("dup", "0"), "1", "0"),
                            MqttQoS.valueOf(Integer.parseInt(map.getOrDefault("qos", "0"))),
                            BooleanUtils.toBoolean(map.getOrDefault("retain", "0"), "1", "0"),
                            0
                    ),
                    new MqttAdditionalHeader(
                            MqttVersion.valueOf(map.getOrDefault("version", MqttVersion.MQTT_3_1_1.toString())),
                            map.get("clientId"),
                            map.get("userName"),
                            null
                    ),
                    MqttPublishVariableHeader.from(
                            map.get("topicName"),
                            Integer.parseInt(map.getOrDefault("packetId", "0"))
                    ),
                    new MqttPublishPayload(
                            bytes
                    ));
        } else if (type == MqttMessageType.PUBREL.value()) {
            return new Message<>(
                    new MqttFixedHeader(
                            MqttMessageType.PUBREL,
                            false,
                            MqttQoS.AT_LEAST_ONCE,
                            false,
                            0
                    ),
                    new MqttAdditionalHeader(
                            MqttVersion.valueOf(map.getOrDefault("version", MqttVersion.MQTT_3_1_1.toString())),
                            map.get("clientId"),
                            map.get("userName"),
                            null
                    ),
                    MqttPacketIdVariableHeader.from(
                            Integer.parseInt(map.getOrDefault("packetId", "0"))
                    ),
                    null
            );
        } else {
            throw new IllegalArgumentException("Invalid in-flight MQTT message type: " + MqttMessageType.valueOf(type));
        }
    }

    /**
     * Convert (MQTT) Message to Map
     *
     * @param msg (MQTT) Message
     * @return Map
     */
    public static Map<String, String> messageToMap(Message msg) {
        Map<String, String> map = new HashMap<>();
        if (msg == null) return map;

        if (msg.fixedHeader().messageType() == MqttMessageType.PUBLISH) {
            MqttPublishVariableHeader variableHeader = (MqttPublishVariableHeader) msg.variableHeader();
            MqttPublishPayload payload = (MqttPublishPayload) msg.payload();
            map.put("type", String.valueOf(MqttMessageType.PUBLISH.value()));
            map.put("retain", BooleanUtils.toString(msg.fixedHeader().retain(), "1", "0"));
            map.put("qos", String.valueOf(msg.fixedHeader().qos().value()));
            map.put("dup", BooleanUtils.toString(msg.fixedHeader().dup(), "1", "0"));
            map.put("version", msg.additionalHeader().version().toString());
            if (!msg.fixedHeader().retain()) map.put("clientId", msg.additionalHeader().clientId());
            map.put("userName", msg.additionalHeader().userName());
            map.put("topicName", variableHeader.topicName());
            if (!msg.fixedHeader().retain()) map.put("packetId", String.valueOf(variableHeader.packetId()));
            if (payload.bytes() != null && payload.bytes().length > 0) try {
                map.put("payload", new String(payload.bytes(), "ISO-8859-1"));
            } catch (UnsupportedEncodingException ignore) {
            }
            return map;
        } else if (msg.fixedHeader().messageType() == MqttMessageType.PUBREL) {
            MqttPacketIdVariableHeader variableHeader = (MqttPacketIdVariableHeader) msg.variableHeader();
            map.put("type", String.valueOf(MqttMessageType.PUBREL.value()));
            map.put("version", msg.additionalHeader().version().toString());
            map.put("clientId", msg.additionalHeader().clientId());
            map.put("userName", msg.additionalHeader().userName());
            map.put("packetId", String.valueOf(variableHeader.packetId()));
            return map;
        } else {
            throw new IllegalArgumentException("Invalid in-flight MQTT message type: " + msg.fixedHeader().messageType());
        }
    }
}
