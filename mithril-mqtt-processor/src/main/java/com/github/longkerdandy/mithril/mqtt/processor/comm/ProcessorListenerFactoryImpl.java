package com.github.longkerdandy.mithril.mqtt.processor.comm;

import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorListener;
import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorListenerFactory;

/**
 * Processor Listener Factory Implementation
 */
public class ProcessorListenerFactoryImpl implements ProcessorListenerFactory {

    @Override
    public ProcessorListener newListener(ProcessorCommunicator communicator) {
        return new ProcessorListenerImpl();
    }
}
