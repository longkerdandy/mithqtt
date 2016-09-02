package com.github.longkerdandy.mithqtt.broker.cluster;

import com.github.longkerdandy.mithqtt.api.cluster.ClusterListener;
import com.github.longkerdandy.mithqtt.api.cluster.ClusterListenerFactory;
import com.github.longkerdandy.mithqtt.broker.session.SessionRegistry;

/**
 * Broker Cluster Listener Factory Implementation
 */
public class BrokerClusterListenerFactoryImpl implements ClusterListenerFactory {

    private final SessionRegistry registry;

    public BrokerClusterListenerFactoryImpl(SessionRegistry registry) {
        this.registry = registry;
    }

    @Override
    public ClusterListener newListener() {
        return new BrokerClusterListenerImpl(registry);
    }
}
