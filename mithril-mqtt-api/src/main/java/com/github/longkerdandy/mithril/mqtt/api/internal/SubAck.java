package com.github.longkerdandy.mithril.mqtt.api.internal;

import io.netty.handler.codec.mqtt.MqttSubAckReturnCode;

import java.util.List;

/**
 * Represent MQTT SUBACK Message's VariableHeader and Payload
 */
public class SubAck {

    private int packetId;
    private List<MqttSubAckReturnCode> grantedQoSLevels;

    public int getPacketId() {
        return packetId;
    }

    public void setPacketId(int packetId) {
        this.packetId = packetId;
    }

    public List<MqttSubAckReturnCode> getGrantedQoSLevels() {
        return grantedQoSLevels;
    }

    public void setGrantedQoSLevels(List<MqttSubAckReturnCode> grantedQoSLevels) {
        this.grantedQoSLevels = grantedQoSLevels;
    }
}
