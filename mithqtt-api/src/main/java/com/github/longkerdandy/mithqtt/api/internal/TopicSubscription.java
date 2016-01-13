package com.github.longkerdandy.mithqtt.api.internal;

import io.netty.handler.codec.mqtt.MqttGrantedQoS;

import java.io.Serializable;

/**
 * Contains a topic name and granted Qos Level.
 * This is part of the {@link Subscribe}
 */
@SuppressWarnings("unused")
public class TopicSubscription implements Serializable {

    private String topic;
    private MqttGrantedQoS grantedQos;

    protected TopicSubscription() {
    }

    public TopicSubscription(String topic, MqttGrantedQoS grantedQos) {
        this.topic = topic;
        this.grantedQos = grantedQos;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public MqttGrantedQoS getGrantedQos() {
        return grantedQos;
    }

    public void setGrantedQos(MqttGrantedQoS grantedQos) {
        this.grantedQos = grantedQos;
    }
}
