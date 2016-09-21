package com.github.longkerdandy.mithqtt.api.storage.sync;

/**
 * MQTT Client Connection State
 */
public enum ConnectionState {
    DISCONNECTED(-1),
    DISCONNECTING(0),
    CONNECTING(1),
    CONNECTED(2);

    private final int value;

    ConnectionState(int value) {
        this.value = value;
    }

    public static ConnectionState valueOf(int value) {
        for (ConnectionState s : values()) {
            if (s.value == value) {
                return s;
            }
        }
        throw new IllegalArgumentException("invalid connection state: " + value);
    }

    public int value() {
        return value;
    }
}
