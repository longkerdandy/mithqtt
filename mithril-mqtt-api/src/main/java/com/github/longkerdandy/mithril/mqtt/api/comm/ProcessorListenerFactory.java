package com.github.longkerdandy.mithril.mqtt.api.comm;

/**
 * Processor Listener Factory
 */
public interface ProcessorListenerFactory {

    /**
     * Create a new ProcessorListener
     *
     * @return ProcessorListener
     */
    ProcessorListener newListener(ProcessorCommunicator communicator);
}
