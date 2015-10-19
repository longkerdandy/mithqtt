package com.github.longkerdandy.mithril.mqtt.api.comm;

import com.github.longkerdandy.mithril.mqtt.api.internal.*;

/**
 * Processor's Listener for Processor Communicator
 */
@SuppressWarnings("unused")
public interface ProcessorListener {

    /**
     * Handle internal connect message
     *
     * @param msg InternalMessage<Connect>
     */
    void onConnect(InternalMessage<Connect> msg);

    /**
     * Handle internal publish message
     *
     * @param msg InternalMessage<Publish>
     */
    void onPublish(InternalMessage<Publish> msg);

    /**
     * Handle internal subscribe message
     *
     * @param msg InternalMessage<Subscribe>
     */
    void onSubscribe(InternalMessage<Subscribe> msg);

    /**
     * Handle internal unsubscribe message
     *
     * @param msg InternalMessage<Unsubscribe>
     */
    void onUnsubscribe(InternalMessage<Unsubscribe> msg);

    /**
     * Handle internal disconnect message
     *
     * @param msg InternalMessage<Disconnect>
     */
    void onDisconnect(InternalMessage<Disconnect> msg);
}
