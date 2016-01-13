package com.github.longkerdandy.mithqtt.broker.comm;

import com.github.longkerdandy.mithqtt.api.internal.Publish;
import com.github.longkerdandy.mithqtt.api.comm.BrokerListener;
import com.github.longkerdandy.mithqtt.api.internal.Disconnect;
import com.github.longkerdandy.mithqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithqtt.broker.session.SessionRegistry;
import io.netty.channel.ChannelHandlerContext;
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
        logger.trace("Message forward: Send PUBLISH message to client {}", msg.getClientId());
        this.registry.sendMessage(msg.toMqttMessage(), msg.getClientId(), msg.getPayload().getPacketId(), true);
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
