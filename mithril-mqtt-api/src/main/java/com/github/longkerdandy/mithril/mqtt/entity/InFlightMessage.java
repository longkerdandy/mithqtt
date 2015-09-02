package com.github.longkerdandy.mithril.mqtt.entity;

/**
 * In-Flight MQTT PUBLISH/PUBREL Message
 */
public class InFlightMessage {

    public static final int TYPE_PUBLISH = 3;
    public static final int TYPE_PUBREL = 6;

    // MQTT message type
    private int type;

    private boolean retain;
    private int qos;
    private boolean dup;
    private String topicName;
    private int packetId;
    private byte[] payload;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public boolean isRetain() {
        return retain;
    }

    public void setRetain(boolean retain) {
        this.retain = retain;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public boolean isDup() {
        return dup;
    }

    public void setDup(boolean dup) {
        this.dup = dup;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getPacketId() {
        return packetId;
    }

    public void setPacketId(int packetId) {
        this.packetId = packetId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }
}
