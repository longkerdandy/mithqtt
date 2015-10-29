package com.github.longkerdandy.mithril.mqtt.api.comm;

/**
 * Application Listener Factory
 */
@SuppressWarnings("unused")
public interface ApplicationListenerFactory {

    /**
     * Create a new ApplicationListener
     *
     * @return ApplicationListener
     */
    ApplicationListener newListener();
}
