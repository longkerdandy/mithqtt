package com.github.longkerdandy.mithril.mqtt.api.comm;

/**
 * Communicator Utils
 */
@SuppressWarnings("unused")
public class Communicators {

    // Topic for Processor
    public static final String PROCESSOR = "mithril.mqtt.processor";
    // Topic for 3rd Party Message Handler
    public static final String MESSAGE = "mithril.mqtt.message";

    private Communicators() {
    }

    // Topic for Broker
    public static String BROKER(String brokerId) {
        return "mithril.mqtt.broker." + brokerId;
    }
}
