package com.github.longkerdandy.mithril.mqtt.api.metrics;

/**
 * Message Direction
 */
public enum MessageDirection {
    IN(1),
    OUT(0);

    private final int value;

    MessageDirection(int value) {
        this.value = value;
    }

    public static MessageDirection valueOf(int direction) {
        for (MessageDirection d : values()) {
            if (d.value == direction) {
                return d;
            }
        }
        throw new IllegalArgumentException("unknown message direction: " + direction);
    }

    public int value() {
        return value;
    }
}
