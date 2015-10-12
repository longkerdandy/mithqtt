package com.github.longkerdandy.mithril.mqtt.api.comm;

/**
 * Represent MQTT PUBLISH Message's VariableHeader and Payload
 */
public class CommunicatorPublishPayload {

    private String topicName;
    private int packetId;
    private byte[] payload;

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
