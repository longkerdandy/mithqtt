package com.github.longkerdandy.mithril.mqtt.broker.comm;

import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerListener;
import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.api.internal.Publish;

/**
 * Broker Listener Implementation
 */
public class BrokerListenerImpl implements BrokerListener {

    @Override
    public void onPublish(InternalMessage<Publish> msg) {

    }

    @Override
    public void onDisconnect(InternalMessage msg) {

    }
}
