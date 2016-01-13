package com.github.longkerdandy.mithqtt.broker.comm;

import com.github.longkerdandy.mithqtt.api.comm.BrokerListener;
import com.github.longkerdandy.mithqtt.api.comm.BrokerListenerFactory;
import com.github.longkerdandy.mithqtt.broker.session.SessionRegistry;

/**
 * Broker Listener Factory Implementation
 */
public class BrokerListenerFactoryImpl implements BrokerListenerFactory {

    private final SessionRegistry registry;

    public BrokerListenerFactoryImpl(SessionRegistry registry) {
        this.registry = registry;
    }

    @Override
    public BrokerListener newListener() {
        return new BrokerListenerImpl(this.registry);
    }
}
