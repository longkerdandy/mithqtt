package com.github.longkerdandy.mithril.mqtt.api.comm;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.api.internal.Publish;

/**
 * Broker's Listener for Broker Communicator
 */
public interface BrokerListener {

    /**
     * Handle PUBLISH message
     *
     * @param msg InternalMessage<Publish>
     */
    void onPublish(InternalMessage<Publish> msg);

    /**
     * Handle DISCONNECT message
     *
     * @param msg InternalMessage
     */
    void onDisconnect(InternalMessage msg);
}
