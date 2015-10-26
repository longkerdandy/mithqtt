package com.github.longkerdandy.mithril.mqtt.api.comm;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Processor Communicator
 * Communicate between processors and brokers
 */
@SuppressWarnings("unused")
public interface ProcessorCommunicator {

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

    /**
     * Send internal message to broker
     *
     * @param brokerId Broker Id
     * @param message  Internal Message
     */
    void sendToBroker(String brokerId, InternalMessage message);

    /**
     * Send internal message to bridge
     * 3rd party program can handle the message from there
     *
     * @param message Internal Message
     */
    void sendToBridge(InternalMessage message);

    /**
     * Send internal message to given topic
     *
     * @param topic   Topic
     * @param message Internal Message
     */
    void sendToTopic(String topic, InternalMessage message);
}
