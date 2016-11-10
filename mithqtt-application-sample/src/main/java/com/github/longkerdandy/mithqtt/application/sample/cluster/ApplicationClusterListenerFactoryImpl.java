package com.github.longkerdandy.mithqtt.application.sample.cluster;

import com.github.longkerdandy.mithqtt.api.cluster.ClusterListener;
import com.github.longkerdandy.mithqtt.api.cluster.ClusterListenerFactory;

/**
 * Broker Cluster Listener Factory Implementation
 */
public class ApplicationClusterListenerFactoryImpl implements ClusterListenerFactory {

    @Override
    public ClusterListener newListener() {
        return new ApplicationClusterListenerImpl();
    }
}
