package com.github.longkerdandy.mithqtt.api.cluster;

import com.github.longkerdandy.mithqtt.api.message.Message;
import com.github.longkerdandy.mithqtt.api.message.MqttPublishPayload;
import com.github.longkerdandy.mithqtt.api.message.MqttSubscribePayloadGranted;
import io.netty.handler.codec.mqtt.*;

/**
 * Cluster Event Listener
 */
public interface ClusterListener {

    /**
     * Received Connect Message Event
     *
     * @param msg Connect Message
     */
    void onConnect(Message<MqttConnectVariableHeader, MqttConnectPayload> msg);

    /**
     * Received Subscribe Message Event
     *
     * @param msg Subscribe Message
     */
    void onSubscribe(Message<MqttPacketIdVariableHeader, MqttSubscribePayloadGranted> msg);

    /**
     * Received Unsubscribe Message Event
     *
     * @param msg Unsubscribe Message
     */
    void onUnsubscribe(Message<MqttPacketIdVariableHeader, MqttUnsubscribePayload> msg);

    /**
     * Received Publish Message Event
     *
     * @param msg Publish Message
     */
    void onPublish(Message<MqttPublishVariableHeader, MqttPublishPayload> msg);

    /**
     * Received Disconnect Message Event
     *
     * @param msg Disconnect Message
     */
    void onDisconnect(Message<Void, Void> msg);
}
