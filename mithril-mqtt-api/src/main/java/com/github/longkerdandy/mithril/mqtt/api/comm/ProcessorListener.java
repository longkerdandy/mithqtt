package com.github.longkerdandy.mithril.mqtt.api.comm;

import com.github.longkerdandy.mithril.mqtt.api.internal.*;

/**
 * Processor's Listener for Processor Communicator
 */
public interface ProcessorListener {

    /**
     * Handle CONNECT message
     *
     * @param msg InternalMessage<Connect>
     */
    void onConnect(InternalMessage<Connect> msg);

    /**
     * Handle PUBLISH message
     *
     * @param msg InternalMessage<Publish>
     */
    void onPublish(InternalMessage<Publish> msg);

    /**
     * Handle SUBSCRIBE message
     *
     * @param msg InternalMessage<Subscribe>
     */
    void onSubscribe(InternalMessage<Subscribe> msg);

    /**
     * Handle UNSUBSCRIBE message
     *
     * @param msg InternalMessage<Unsubscribe>
     */
    void onUnsubscribe(InternalMessage<Unsubscribe> msg);

    /**
     * Handle DISCONNECT message
     *
     * @param msg InternalMessage
     */
    void onDisconnect(InternalMessage msg);
}
