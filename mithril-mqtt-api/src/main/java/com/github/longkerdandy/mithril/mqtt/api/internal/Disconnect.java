package com.github.longkerdandy.mithril.mqtt.api.internal;

/**
 * Represent MQTT DISCONNECT Message
 */
public class Disconnect {

    private boolean clearExit;

    public boolean isClearExit() {
        return clearExit;
    }

    public void setClearExit(boolean clearExit) {
        this.clearExit = clearExit;
    }
}
