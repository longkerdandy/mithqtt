package com.github.longkerdandy.mithqtt.api.internal;

import java.io.Serializable;
import java.util.List;

/**
 * Represent MQTT UNSUBSCRIBE Message's VariableHeader and Payload
 */
@SuppressWarnings("unused")
public class Unsubscribe implements Serializable {

    private int packetId;
    private List<String> topics;

    protected Unsubscribe() {
    }

    public Unsubscribe(int packetId, List<String> topics) {
        this.packetId = packetId;
        this.topics = topics;
    }

    public int getPacketId() {
        return packetId;
    }

    public void setPacketId(int packetId) {
        this.packetId = packetId;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }
}
