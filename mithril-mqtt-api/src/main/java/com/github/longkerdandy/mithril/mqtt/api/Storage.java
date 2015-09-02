package com.github.longkerdandy.mithril.mqtt.api;

import com.github.longkerdandy.mithril.mqtt.entity.InFlightMessage;

import java.util.List;

/**
 * Storage
 */
public interface Storage {

    /**
     * Remove subscriptions for the client
     *
     * @param clientId Client Id
     * @return Number of subscriptions that removed
     */
    int removeSubscriptions(String clientId);

    /**
     * Get all in-flight messages for the client
     * Including£º
     * QoS 1 and QoS 2 PUBLISH messages which have been sent to the Client, but have not been acknowledged.
     * QoS 0, QoS 1 and QoS 2 PUBLISH messages pending transmission to the Client.
     * QoS 2 PUBREL messages which have been sent from the Client, but have not been acknowledged.
     *
     * @param clientId Client Id
     * @return List of InFlightMessage in order
     */
    List<InFlightMessage> getInFlightMessages(String clientId);

    /**
     * Remove all in-flight messages for the client
     * Including£º
     * QoS 1 and QoS 2 PUBLISH messages which have been sent to the Client, but have not been acknowledged.
     * QoS 0, QoS 1 and QoS 2 PUBLISH messages pending transmission to the Client.
     * QoS 2 PUBREL messages which have been sent from the Client, but have not been acknowledged.
     *
     * @param clientId Client Id
     * @return Number of messages that removed
     */
    int removeInFlightMessages(String clientId);
}
