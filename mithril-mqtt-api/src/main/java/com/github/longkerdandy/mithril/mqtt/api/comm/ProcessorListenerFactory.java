package com.github.longkerdandy.mithril.mqtt.api.comm;

/**
 * ProcessorListenerF actory
 */
public interface ProcessorListenerFactory {

    /**
     * Create a new ProcessorListener
     *
     * @return ProcessorListener
     */
    ProcessorListener newListener();
}
