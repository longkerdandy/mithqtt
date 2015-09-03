package com.github.longkerdandy.mithril.mqtt.broker.util;

import com.github.longkerdandy.mithril.mqtt.entity.InFlightMessage;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;

import static io.netty.buffer.Unpooled.wrappedBuffer;

/**
 * Converter
 */
public class Converter {

    private Converter() {
    }

    /**
     * Convert MqttMessage to InFlightMessage
     *
     * @param mqtt MqttMessage
     * @return InFlightMessage
     */
    public static InFlightMessage mqttToInFlight(MqttMessage mqtt) {
        if (mqtt.fixedHeader().messageType() == MqttMessageType.PUBLISH) {
            InFlightMessage inFlight = new InFlightMessage();
            inFlight.setType(InFlightMessage.TYPE_PUBLISH);
            inFlight.setDup(mqtt.fixedHeader().isDup());
            inFlight.setQos(mqtt.fixedHeader().qosLevel().value());
            inFlight.setRetain(mqtt.fixedHeader().isRetain());
            inFlight.setTopicName(((MqttPublishVariableHeader) mqtt.variableHeader()).topicName());
            inFlight.setPacketId(((MqttPublishVariableHeader) mqtt.variableHeader()).messageId());
            inFlight.setPayload(((ByteBuf) mqtt.payload()).array());
            return inFlight;
        } else if (mqtt.fixedHeader().messageType() == MqttMessageType.PUBREL) {
            InFlightMessage inFlight = new InFlightMessage();
            inFlight.setType(InFlightMessage.TYPE_PUBREL);
            inFlight.setQos(1);
            inFlight.setPacketId(((MqttMessageIdVariableHeader) mqtt.variableHeader()).messageId());
            return inFlight;
        } else {
            throw new IllegalArgumentException("Invalid in-flight MQTT message type: " + mqtt.fixedHeader().messageType());
        }
    }

    /**
     * Convert InFlightMessage to MqttMessage
     *
     * @param inFlight InFlightMessage
     * @return MqttMessage
     */
    public static MqttMessage inFlightToMqtt(InFlightMessage inFlight) {
        if (inFlight.getType() == InFlightMessage.TYPE_PUBLISH) {
            return MqttMessageFactory.newMessage(
                    new MqttFixedHeader(
                            MqttMessageType.PUBLISH,
                            inFlight.isDup(),
                            MqttQoS.valueOf(inFlight.getQos()),
                            inFlight.isRetain(),
                            0
                    ),
                    new MqttPublishVariableHeader(inFlight.getTopicName(), inFlight.getPacketId()),
                    wrappedBuffer(inFlight.getPayload())
            );
        } else if (inFlight.getType() == InFlightMessage.TYPE_PUBREL) {
            return MqttMessageFactory.newMessage(
                    new MqttFixedHeader(
                            MqttMessageType.PUBREL,
                            false,
                            MqttQoS.AT_LEAST_ONCE,
                            false,
                            0
                    ),
                    MqttMessageIdVariableHeader.from(inFlight.getPacketId()),
                    null
            );
        } else {
            throw new IllegalArgumentException("Invalid in-flight MQTT message type: " + inFlight.getType());
        }
    }
}
