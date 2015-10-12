package com.github.longkerdandy.mithril.mqtt.api.comm;

import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Processor Communicator
 * Communicate between processors and brokers
 */
public interface ProcessorCommunicator {

    String TOPIC = "mithril.mqtt.processor";

    /**
     * Init the communicator
     *
     * @param config  Communicator Configuration
     * @param factory Processor Listener Factory
     */
    void init(PropertiesConfiguration config, ProcessorListenerFactory factory);

    /**
     * Destroy the communicator
     */
    void destroy();
}
