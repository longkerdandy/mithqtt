package com.github.longkerdandy.mithqtt.api.message;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.util.internal.StringUtil;

/**
 * Message
 */
public class Message<V, P> {

    protected MqttFixedHeader fixedHeader;
    protected MqttAdditionalHeader additionalHeader;
    protected V variableHeader;
    protected P payload;

    public Message(MqttFixedHeader fixedHeader, MqttAdditionalHeader additionalHeader, V variableHeader, P payload) {
        this.fixedHeader = fixedHeader;
        this.additionalHeader = additionalHeader;
        this.variableHeader = variableHeader;
        this.payload = payload;
    }

    public MqttFixedHeader fixedHeader() {
        return fixedHeader;
    }

    public MqttAdditionalHeader additionalHeader() {
        return additionalHeader;
    }

    public V variableHeader() {
        return variableHeader;
    }

    public P payload() {
        return payload;
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this)
                + '['
                + "fixedHeader=" + (fixedHeader != null ? fixedHeader.toString() : "")
                + ", additionalHeader=" + (additionalHeader != null ? additionalHeader.toString() : "")
                + ", variableHeader=" + (variableHeader != null ? variableHeader.toString() : "")
                + ", payload=" + (payload != null ? payload.toString() : "")
                + ']';
    }
}
