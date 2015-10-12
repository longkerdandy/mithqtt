package com.github.longkerdandy.mithril.mqtt.api.comm;

/**
 * Represent MQTT Message's VariableHeader which only contains Packet Id
 */
public class CommunicatorPacketIdPayload {

    private int packetId;

    public int getPacketId() {
        return packetId;
    }

    public void setPacketId(int packetId) {
        this.packetId = packetId;
    }
}
