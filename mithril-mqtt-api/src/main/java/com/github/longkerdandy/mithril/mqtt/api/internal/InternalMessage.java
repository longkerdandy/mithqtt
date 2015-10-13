package com.github.longkerdandy.mithril.mqtt.api.internal;

import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;

/**
 * Represent MQTT Message passed in Communicator
 */
public class InternalMessage<T> {

    // fixed header
    private MqttMessageType messageType;
    private boolean dup;
    private MqttQoS qos;
    private boolean retain;

    // some info in CONNECT message, but useful for stateless transfer
    private MqttVersion version;
    private boolean cleanSession;
    private String clientId;
    private String userName;

    // variable header and payload
    private T payload;

    public MqttMessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MqttMessageType messageType) {
        this.messageType = messageType;
    }

    public boolean isDup() {
        return dup;
    }

    public void setDup(boolean dup) {
        this.dup = dup;
    }

    public MqttQoS getQos() {
        return qos;
    }

    public void setQos(MqttQoS qos) {
        this.qos = qos;
    }

    public boolean isRetain() {
        return retain;
    }

    public void setRetain(boolean retain) {
        this.retain = retain;
    }

    public MqttVersion getVersion() {
        return version;
    }

    public void setVersion(MqttVersion version) {
        this.version = version;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

    public void cloneFields(InternalMessage msg) {
        this.messageType = msg.messageType;
        this.dup = msg.dup;
        this.qos = msg.qos;
        this.retain = msg.retain;
        this.version = msg.version;
        this.cleanSession = msg.cleanSession;
        this.clientId = msg.clientId;
        this.userName = msg.userName;
    }
}
