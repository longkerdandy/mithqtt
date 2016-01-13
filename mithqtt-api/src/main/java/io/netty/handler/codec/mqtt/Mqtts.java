package io.netty.handler.codec.mqtt;

import org.apache.commons.lang3.StringUtils;

import static com.github.longkerdandy.mithqtt.util.UUIDs.shortUuid;

/**
 * MQTT Utils
 */
public class Mqtts {

    /**
     * Sanitize MQTT message
     *
     * @param msg MQTT message
     */
    public static void sanitize(MqttMessage msg) {
        switch (msg.fixedHeader().messageType()) {
            case CONNECT:
                if (StringUtils.isBlank(((MqttConnectMessage) msg).payload().clientId())
                        && ((MqttConnectMessage) msg).variableHeader().cleanSession())
                    ((MqttConnectMessage) msg).payload().clientId = shortUuid();
            case CONNACK:
            case PUBACK:
            case PUBREC:
            case PUBCOMP:
            case SUBACK:
            case UNSUBACK:
            case PINGREQ:
            case PINGRESP:
            case DISCONNECT:
                msg.fixedHeader().dup = false;
                msg.fixedHeader().qos = MqttQoS.AT_MOST_ONCE;
                msg.fixedHeader().retain = false;
                break;
            case PUBREL:
            case SUBSCRIBE:
            case UNSUBSCRIBE:
                msg.fixedHeader().dup = false;
                msg.fixedHeader().qos = MqttQoS.AT_LEAST_ONCE;
                msg.fixedHeader().retain = false;
                break;
            case PUBLISH:
                if (msg.fixedHeader().qos == MqttQoS.AT_MOST_ONCE)
                    ((MqttPublishVariableHeader) msg.variableHeader()).packetId = 0;
                break;
        }
    }
}
