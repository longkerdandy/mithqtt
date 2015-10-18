package com.github.longkerdandy.mithril.mqtt.api.internal;

import io.netty.handler.codec.mqtt.MqttSubAckReturnCode;
import io.netty.util.internal.StringUtil;

/**
 * Contains a topic name and granted Qos Level.
 * This is part of the {@link Subscribe}
 */
public class TopicSubscription {

    private final String topic;
    private final MqttSubAckReturnCode grantedQos;

    public TopicSubscription(String topic, MqttSubAckReturnCode grantedQos) {
        this.topic = topic;
        this.grantedQos = grantedQos;
    }

    public String topic() {
        return topic;
    }

    public MqttSubAckReturnCode requestedQos() {
        return grantedQos;
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this)
                + '['
                + "topic=" + topic
                + ", requestedQos=" + grantedQos
                + ']';
    }
}
