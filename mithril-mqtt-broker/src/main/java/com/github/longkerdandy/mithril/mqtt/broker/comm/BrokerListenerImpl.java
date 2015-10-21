package com.github.longkerdandy.mithril.mqtt.broker.comm;

import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerListener;
import com.github.longkerdandy.mithril.mqtt.api.internal.Disconnect;
import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.api.internal.Publish;
import com.github.longkerdandy.mithril.mqtt.broker.session.SessionRegistry;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker Listener Implementation
 */
public class BrokerListenerImpl implements BrokerListener {

    private static final Logger logger = LoggerFactory.getLogger(BrokerListenerImpl.class);

    private final SessionRegistry registry;

    public BrokerListenerImpl(SessionRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void onPublish(InternalMessage<Publish> msg) {
        MqttMessage m = msg.toMqttMessage();
        this.registry.sendMessage(m, msg.getClientId(), msg.getPayload().getPacketId(), true);
    }

    @Override
    public void onDisconnect(InternalMessage<Disconnect> msg) {
        ChannelHandlerContext ctx = this.registry.removeSession(msg.getClientId());
        if (ctx != null) {
            logger.trace("Disconnect: Try to disconnect existed client {}", msg.getClientId());
            ctx.close();
        }
    }
}
