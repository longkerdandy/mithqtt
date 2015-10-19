package com.github.longkerdandy.mithril.mqtt.api.internal;

/**
 * Represent MQTT DISCONNECT Message
 */
@SuppressWarnings("unused")
public class Disconnect {

    // clean exit means client sent DISCONNECT before closing the connection
    private boolean clean;

    protected Disconnect() {
    }

    public Disconnect(boolean clean) {
        this.clean = clean;
    }

    public boolean isClean() {
        return clean;
    }

    public void setClean(boolean clean) {
        this.clean = clean;
    }
}
