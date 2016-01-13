package com.github.longkerdandy.mithqtt.storage.redis.util;

import com.github.longkerdandy.mithqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithqtt.api.internal.Publish;
import com.github.longkerdandy.mithqtt.api.internal.PacketId;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.apache.commons.lang3.BooleanUtils;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 * Converter Utils
 */
public class Converter {

    /**
     * Convert Map to InternalMessage
     *
     * @param map Map
     * @return InternalMessage
     */
    public static InternalMessage mapToInternal(Map<String, String> map) {
        if (map == null || map.isEmpty()) return null;

        int type = Integer.parseInt(map.get("type"));
        if (type == MqttMessageType.PUBLISH.value()) {
            byte[] payload = null;
            if (map.get("payload") != null) try {
                payload = map.get("payload").getBytes("ISO-8859-1");
            } catch (UnsupportedEncodingException ignore) {
            }
            return new InternalMessage<>(
                    MqttMessageType.PUBLISH,
                    BooleanUtils.toBoolean(map.getOrDefault("dup", "0"), "1", "0"),
                    MqttQoS.valueOf(Integer.parseInt(map.getOrDefault("qos", "0"))),
                    BooleanUtils.toBoolean(map.getOrDefault("retain", "0"), "1", "0"),
                    MqttVersion.valueOf(map.getOrDefault("version", MqttVersion.MQTT_3_1_1.toString())),
                    map.get("clientId"),
                    map.get("userName"),
                    null,
                    new Publish(
                            map.get("topicName"),
                            Integer.parseInt(map.getOrDefault("packetId", "0")),
                            payload
                    ));
        } else if (type == MqttMessageType.PUBREL.value()) {
            return new InternalMessage<>(
                    MqttMessageType.PUBREL,
                    false,
                    MqttQoS.AT_LEAST_ONCE,
                    false,
                    MqttVersion.valueOf(map.getOrDefault("version", MqttVersion.MQTT_3_1_1.toString())),
                    map.get("clientId"),
                    map.get("userName"),
                    null,
                    new PacketId(Integer.parseInt(map.getOrDefault("packetId", "0"))));
        } else {
            throw new IllegalArgumentException("Invalid in-flight MQTT message type: " + MqttMessageType.valueOf(type));
        }
    }

    /**
     * Convert InternalMessage to Map
     *
     * @param msg InternalMessage
     * @return Map
     */
    public static Map<String, String> internalToMap(InternalMessage msg) {
        Map<String, String> map = new HashMap<>();
        if (msg == null) return map;

        if (msg.getMessageType() == MqttMessageType.PUBLISH) {
            Publish publish = (Publish) msg.getPayload();
            map.put("type", String.valueOf(MqttMessageType.PUBLISH.value()));
            map.put("retain", BooleanUtils.toString(msg.isRetain(), "1", "0"));
            map.put("qos", String.valueOf(msg.getQos().value()));
            map.put("dup", BooleanUtils.toString(msg.isDup(), "1", "0"));
            map.put("version", msg.getVersion().toString());
            if (!msg.isRetain()) map.put("clientId", msg.getClientId());
            map.put("userName", msg.getUserName());
            map.put("topicName", publish.getTopicName());
            if (!msg.isRetain()) map.put("packetId", String.valueOf(publish.getPacketId()));
            if (publish.getPayload() != null && publish.getPayload().length > 0) try {
                map.put("payload", new String(publish.getPayload(), "ISO-8859-1"));
            } catch (UnsupportedEncodingException ignore) {
            }
            return map;
        } else if (msg.getMessageType() == MqttMessageType.PUBREL) {
            PacketId packetId = (PacketId) msg.getPayload();
            map.put("type", String.valueOf(MqttMessageType.PUBREL.value()));
            map.put("version", msg.getVersion().toString());
            map.put("clientId", msg.getClientId());
            map.put("userName", msg.getUserName());
            map.put("packetId", String.valueOf(packetId.getPacketId()));
            return map;
        } else {
            throw new IllegalArgumentException("Invalid in-flight MQTT message type: " + msg.getMessageType());
        }
    }
}
