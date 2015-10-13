package com.github.longkerdandy.mithril.mqtt.api.internal;

/**
 * Represent MQTT Message's VariableHeader which only contains Packet Id
 */
public class PacketId {

    private int packetId;

    public int getPacketId() {
        return packetId;
    }

    public void setPacketId(int packetId) {
        this.packetId = packetId;
    }
}
