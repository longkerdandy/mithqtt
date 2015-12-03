package com.github.longkerdandy.mithril.mqtt.http;

import io.dropwizard.Configuration;

import javax.validation.constraints.NotNull;

/**
 * MqttHttp Configuration
 */
@SuppressWarnings("unused")
public class MqttHttpConfiguration extends Configuration {

    @NotNull
    private String serverId;

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }
}
