package com.github.longkerdandy.mithqtt.api.cluster;

/**
 * Cluster Listener Factory
 */
public interface ClusterListenerFactory {

    /**
     * Create a new ClusterListener
     *
     * @return ClusterListener
     */
    ClusterListener newListener();
}
