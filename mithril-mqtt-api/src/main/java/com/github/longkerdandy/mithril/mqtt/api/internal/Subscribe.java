package com.github.longkerdandy.mithril.mqtt.api.internal;

import io.netty.handler.codec.mqtt.MqttTopicSubscription;

import java.util.List;

/**
 * Represent MQTT SUBSCRIBE Message's VariableHeader and Payload
 */
public class Subscribe {

    private int packetId;
    private List<MqttTopicSubscription> topicSubscriptions;

    public int getPacketId() {
        return packetId;
    }

    public void setPacketId(int packetId) {
        this.packetId = packetId;
    }

    public List<MqttTopicSubscription> getTopicSubscriptions() {
        return topicSubscriptions;
    }

    public void setTopicSubscriptions(List<MqttTopicSubscription> topicSubscriptions) {
        this.topicSubscriptions = topicSubscriptions;
    }
}
