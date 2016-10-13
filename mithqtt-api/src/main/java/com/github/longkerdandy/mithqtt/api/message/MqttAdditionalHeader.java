package com.github.longkerdandy.mithqtt.api.message;

import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.util.internal.StringUtil;

/**
 * Mqtt Additional Header
 */
public class MqttAdditionalHeader {

    protected MqttVersion version;
    protected String clientId;
    protected String userName;
    protected String brokerId;

    private MqttAdditionalHeader() {
    }

    public MqttAdditionalHeader(
            MqttVersion version,
            String clientId,
            String userName,
            String brokerId) {
        this.version = version;
        this.clientId = clientId;
        this.userName = userName;
        this.brokerId = brokerId;
    }

    public MqttVersion version() {
        return version;
    }

    public String clientId() {
        return clientId;
    }

    public String userName() {
        return userName;
    }

    public String brokerId() {
        return brokerId;
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this)
                + '['
                + "version=" + version
                + ", clientId=" + clientId
                + ", userName=" + userName
                + ", brokerId=" + brokerId
                + ']';
    }
}
