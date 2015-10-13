package com.github.longkerdandy.mithril.mqtt.api.comm;

/**
 * Communicator Topics
 */
public class CommunicatorTopics {

    // Topic for Processor
    public static final String PROCESSOR = "mithril.mqtt.processor";
    // Topic for Message Handler
    public static final String MESSAGE = "mithril.mqtt.message";
    // Topic for Broker
    public static String BROKER(String brokerId) {
        return "mithril.mqtt.broker." + brokerId;
    }

    private CommunicatorTopics() {
    }
}
