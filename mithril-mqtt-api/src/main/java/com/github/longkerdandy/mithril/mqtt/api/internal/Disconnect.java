package com.github.longkerdandy.mithril.mqtt.api.internal;

/**
 * Represent MQTT DISCONNECT Message
 */
public class Disconnect {

    // clear exit means client sent DISCONNECT before closing the connection
    private boolean clearExit;

    public boolean isClearExit() {
        return clearExit;
    }

    public void setClearExit(boolean clearExit) {
        this.clearExit = clearExit;
    }
}
