package com.github.longkerdandy.mithril.mqtt.broker.comm;

import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerListener;
import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerListenerFactory;

/**
 * Broker Listener Factory Implementation
 */
public class BrokerListenerFactoryImpl implements BrokerListenerFactory {

    @Override
    public BrokerListener newListener(BrokerCommunicator communicator) {
        return new BrokerListenerImpl();
    }
}
