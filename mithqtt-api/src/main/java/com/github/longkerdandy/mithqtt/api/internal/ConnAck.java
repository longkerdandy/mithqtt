package com.github.longkerdandy.mithqtt.api.internal;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;

import java.io.Serializable;

/**
 * Represent MQTT CONNACK Message's VariableHeader
 */
@SuppressWarnings("unused")
public class ConnAck implements Serializable {

    private MqttConnectReturnCode returnCode;
    private boolean sessionPresent;

    protected ConnAck() {
    }

    public ConnAck(MqttConnectReturnCode returnCode, boolean sessionPresent) {
        this.returnCode = returnCode;
        this.sessionPresent = sessionPresent;
    }

    public MqttConnectReturnCode getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(MqttConnectReturnCode returnCode) {
        this.returnCode = returnCode;
    }

    public boolean isSessionPresent() {
        return sessionPresent;
    }

    public void setSessionPresent(boolean sessionPresent) {
        this.sessionPresent = sessionPresent;
    }
}
