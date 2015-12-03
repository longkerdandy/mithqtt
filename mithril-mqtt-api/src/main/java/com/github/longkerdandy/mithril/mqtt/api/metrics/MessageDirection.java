package com.github.longkerdandy.mithril.mqtt.api.metrics;

/**
 * Message Direction
 */
@SuppressWarnings("unused")
public enum MessageDirection {
    IN,
    OUT;

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }
}
