package com.github.longkerdandy.mithril.mqtt.api.comm;

import org.apache.commons.configuration.AbstractConfiguration;

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
    void init(AbstractConfiguration config, ApplicationListenerFactory factory);

    /**
     * Destroy the communicator
     */
    void destroy();
}
