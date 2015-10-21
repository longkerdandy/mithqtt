package com.github.longkerdandy.mithril.mqtt.broker.comm;

import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerListener;
import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerListenerFactory;
import com.github.longkerdandy.mithril.mqtt.broker.session.SessionRegistry;

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
