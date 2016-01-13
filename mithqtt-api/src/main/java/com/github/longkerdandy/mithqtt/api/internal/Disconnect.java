package com.github.longkerdandy.mithqtt.api.internal;

import java.io.Serializable;

/**
 * Represent MQTT DISCONNECT Message
 */
@SuppressWarnings("unused")
public class Disconnect implements Serializable {

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
