package com.github.longkerdandy.mithril.mqtt.api.internal;

import java.util.List;

/**
 * Represent MQTT SUBSCRIBE Message's VariableHeader and Payload
 */
public class Subscribe {

    private int packetId;
    private List<TopicSubscription> topicSubscriptions;

    public int getPacketId() {
        return packetId;
    }

    public void setPacketId(int packetId) {
        this.packetId = packetId;
    }

    public List<TopicSubscription> getTopicSubscriptions() {
        return topicSubscriptions;
    }

    public void TopicSubscription(List<TopicSubscription> topicSubscriptions) {
        this.topicSubscriptions = topicSubscriptions;
    }
}
