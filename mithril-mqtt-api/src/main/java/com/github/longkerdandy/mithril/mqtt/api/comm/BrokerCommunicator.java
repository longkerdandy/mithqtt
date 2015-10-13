package com.github.longkerdandy.mithril.mqtt.api.comm;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Broker Communicator
 * Communicate between processors and brokers
 */
public interface BrokerCommunicator {

    /**
     * Init the communicator
     *
     * @param config   Communicator Configuration
     * @param brokerId Broker Id
     * @param factory  Broker Listener Factory
     */
    void init(PropertiesConfiguration config, String brokerId, BrokerListenerFactory factory);

    /**
     * Destroy the communicator
     */
    void destroy();

    /**
     * Send message to broker
     *
     * @param brokerId Broker Id
     * @param message  Internal Message
     */
    void sendToBroker(String brokerId, InternalMessage message);

    /**
     * Send message to processor
     *
     * @param message Internal Message
     */
    void sendToProcessor(InternalMessage message);

    /**
     * Send message to given topic
     *
     * @param topic   Topic
     * @param message Internal Message
     */
    void sendToTopic(String topic, InternalMessage message);
}
