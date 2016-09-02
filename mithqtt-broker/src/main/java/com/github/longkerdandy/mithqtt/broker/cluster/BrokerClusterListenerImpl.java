package com.github.longkerdandy.mithqtt.broker.cluster;

import com.github.longkerdandy.mithqtt.api.cluster.ClusterListener;
import com.github.longkerdandy.mithqtt.api.message.Message;
import com.github.longkerdandy.mithqtt.api.message.MqttAdditionalHeader;
import com.github.longkerdandy.mithqtt.api.message.MqttPublishPayload;
import com.github.longkerdandy.mithqtt.api.message.MqttSubscribePayloadGranted;
import com.github.longkerdandy.mithqtt.broker.session.SessionRegistry;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker Cluster Listener Implementation
 */
public class BrokerClusterListenerImpl implements ClusterListener {

    private static final Logger logger = LoggerFactory.getLogger(BrokerClusterListenerImpl.class);

    private final SessionRegistry registry;

    public BrokerClusterListenerImpl(SessionRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void onConnect(Message<MqttConnectVariableHeader, MqttConnectPayload> msg) {
        // do nothing
    }

    @Override
    public void onSubscribe(Message<MqttPacketIdVariableHeader, MqttSubscribePayloadGranted> msg) {
        // do nothing
    }

    @Override
    public void onUnsubscribe(Message<MqttPacketIdVariableHeader, MqttUnsubscribePayload> msg) {
        // do nothing
    }

    @Override
    public void onPublish(Message<MqttPublishVariableHeader, MqttPublishPayload> msg) {
        MqttAdditionalHeader additionalHeader = msg.additionalHeader();
        MqttPublishVariableHeader variableHeader = msg.variableHeader();
        MqttPublishPayload payload = msg.payload();
        MqttMessage mqtt = new MqttPublishMessage(msg.fixedHeader(), variableHeader,
                (payload != null && payload.bytes() != null && payload.bytes().length > 0) ?
                        Unpooled.wrappedBuffer(payload.bytes()) : Unpooled.EMPTY_BUFFER);

        logger.trace("Send PUBLISH message to client {}", additionalHeader.clientId());

        this.registry.sendMessage(mqtt, additionalHeader.clientId(), variableHeader.packetId(), true);
    }

    @Override
    public void onDisconnect(Message<Void, Void> msg) {
        MqttAdditionalHeader additionalHeader = msg.additionalHeader();
        ChannelHandlerContext ctx = this.registry.removeSession(additionalHeader.clientId());
        if (ctx != null) {
            logger.trace("Try to disconnect connected client {}", additionalHeader.clientId());

            ctx.close();
        } else {
            logger.trace("Client {} no longer connected to this node", additionalHeader.clientId());
        }
    }
}
