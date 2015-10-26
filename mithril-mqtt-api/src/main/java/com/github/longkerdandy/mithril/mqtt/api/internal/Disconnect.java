package com.github.longkerdandy.mithril.mqtt.api.internal;

/**
 * Represent MQTT DISCONNECT Message
 */
@SuppressWarnings("unused")
public class Disconnect {

    private boolean cleanSession;
    // cleanExit exit means client sent DISCONNECT before closing the connection
    private boolean cleanExit;

    protected Disconnect() {
    }

    public Disconnect(boolean cleanSession, boolean cleanExit) {
        this.cleanSession = cleanSession;
        this.cleanExit = cleanExit;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public boolean isCleanExit() {
        return cleanExit;
    }

    public void setCleanExit(boolean cleanExit) {
        this.cleanExit = cleanExit;
    }
}
