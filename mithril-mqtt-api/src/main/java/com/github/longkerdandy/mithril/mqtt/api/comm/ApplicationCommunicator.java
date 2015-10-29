package com.github.longkerdandy.mithril.mqtt.api.comm;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Application Communicator
 * Link application to other modules
 */
@SuppressWarnings("unused")
public interface ApplicationCommunicator {

    /**
     * Init the communicator
     *
     * @param config  Communicator Configuration
     * @param factory Application Listener Factory
     */
    void init(PropertiesConfiguration config, ApplicationListenerFactory factory);

    /**
     * Destroy the communicator
     */
    void destroy();

    /**
     * Send internal message to processor
     *
     * @param message Internal Message
     */
    void sendToProcessor(InternalMessage message);
}
