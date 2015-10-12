package com.github.longkerdandy.mithril.mqtt.api.comm;

import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.List;

/**
 * Represent MQTT SUBACK Message's VariableHeader and Payload
 */
public class CommunicatorSubAckPayload {

    private int packetId;
    private List<MqttQoS> grantedQoSLevels;

    public int getPacketId() {
        return packetId;
    }

    public void setPacketId(int packetId) {
        this.packetId = packetId;
    }

    public List<MqttQoS> getGrantedQoSLevels() {
        return grantedQoSLevels;
    }

    public void setGrantedQoSLevels(List<MqttQoS> grantedQoSLevels) {
        this.grantedQoSLevels = grantedQoSLevels;
    }
}
