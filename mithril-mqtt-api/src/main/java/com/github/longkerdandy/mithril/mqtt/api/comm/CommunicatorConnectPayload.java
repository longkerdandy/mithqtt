package com.github.longkerdandy.mithril.mqtt.api.comm;

import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * Represent MQTT CONNECT Message's VariableHeader and Payload
 */
public class CommunicatorConnectPayload {

    private boolean willRetain;
    private MqttQoS willQos;
    private String willTopic;
    private byte[] willMessage;

    public boolean isWillRetain() {
        return willRetain;
    }

    public void setWillRetain(boolean willRetain) {
        this.willRetain = willRetain;
    }

    public MqttQoS getWillQos() {
        return willQos;
    }

    public void setWillQos(MqttQoS willQos) {
        this.willQos = willQos;
    }

    public String getWillTopic() {
        return willTopic;
    }

    public void setWillTopic(String willTopic) {
        this.willTopic = willTopic;
    }

    public byte[] getWillMessage() {
        return willMessage;
    }

    public void setWillMessage(byte[] willMessage) {
        this.willMessage = willMessage;
    }
}
