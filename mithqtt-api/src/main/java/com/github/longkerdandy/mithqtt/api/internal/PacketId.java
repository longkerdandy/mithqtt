package com.github.longkerdandy.mithqtt.api.internal;

import java.io.Serializable;

/**
 * Represent MQTT Message's VariableHeader which only contains Packet Id
 */
@SuppressWarnings("unused")
public class PacketId implements Serializable {

    private int packetId;

    protected PacketId() {
    }

    public PacketId(int packetId) {
        this.packetId = packetId;
    }

    public int getPacketId() {
        return packetId;
    }

    public void setPacketId(int packetId) {
        this.packetId = packetId;
    }
}
