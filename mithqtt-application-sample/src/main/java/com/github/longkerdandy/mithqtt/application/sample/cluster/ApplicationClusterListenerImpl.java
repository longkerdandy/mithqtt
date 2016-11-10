package com.github.longkerdandy.mithqtt.application.sample.cluster;

import com.github.longkerdandy.mithqtt.api.cluster.ClusterListener;
import com.github.longkerdandy.mithqtt.api.message.Message;
import com.github.longkerdandy.mithqtt.api.message.MqttPublishPayload;
import com.github.longkerdandy.mithqtt.api.message.MqttSubscribePayloadGranted;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker Cluster Listener Implementation
 */
public class ApplicationClusterListenerImpl implements ClusterListener {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationClusterListenerImpl.class);

    @Override
    public void onConnect(Message<MqttConnectVariableHeader, MqttConnectPayload> msg) {
        logger.debug("Received CONNECT message {}", msg);
    }

    @Override
    public void onSubscribe(Message<MqttPacketIdVariableHeader, MqttSubscribePayloadGranted> msg) {
        logger.debug("Received SUBSCRIBE message {}", msg);
    }

    @Override
    public void onUnsubscribe(Message<MqttPacketIdVariableHeader, MqttUnsubscribePayload> msg) {
        logger.debug("Received UNSUBSCRIBE message {}", msg);
    }

    @Override
    public void onPublish(Message<MqttPublishVariableHeader, MqttPublishPayload> msg) {
        logger.debug("Received PUBLISH message {}", msg);
    }

    @Override
    public void onDisconnect(Message<Void, Void> msg) {
        logger.debug("Received DISCONNECT message {}", msg);
    }
}
