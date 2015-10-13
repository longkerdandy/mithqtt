package com.github.longkerdandy.mithril.mqtt.api.internal;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;

/**
 * Represent MQTT CONNACK Message's VariableHeader
 */
public class ConnAck {

    private MqttConnectReturnCode connectReturnCode;
    private boolean sessionPresent;

    public MqttConnectReturnCode getConnectReturnCode() {
        return connectReturnCode;
    }

    public void setConnectReturnCode(MqttConnectReturnCode connectReturnCode) {
        this.connectReturnCode = connectReturnCode;
    }

    public boolean isSessionPresent() {
        return sessionPresent;
    }

    public void setSessionPresent(boolean sessionPresent) {
        this.sessionPresent = sessionPresent;
    }
}
