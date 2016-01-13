package com.github.longkerdandy.mithqtt.api.internal;

import java.io.Serializable;
import java.util.List;

/**
 * Represent MQTT SUBSCRIBE Message's VariableHeader and Payload
 */
@SuppressWarnings("unused")
public class Subscribe implements Serializable {

    private int packetId;
    private List<TopicSubscription> subscriptions;

    protected Subscribe() {
    }

    public Subscribe(int packetId, List<TopicSubscription> subscriptions) {
        this.packetId = packetId;
        this.subscriptions = subscriptions;
    }

    public int getPacketId() {
        return packetId;
    }

    public void setPacketId(int packetId) {
        this.packetId = packetId;
    }

    public List<TopicSubscription> getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(List<TopicSubscription> subscriptions) {
        this.subscriptions = subscriptions;
    }
}
