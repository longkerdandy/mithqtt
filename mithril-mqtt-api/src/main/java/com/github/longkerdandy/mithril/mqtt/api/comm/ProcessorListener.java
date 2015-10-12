package com.github.longkerdandy.mithril.mqtt.api.comm;

/**
 * Processor's Listener for Processor Communicator
 */
public interface ProcessorListener {

    void setCommunicator(Communicator communicator);

    void onConnect(CommunicatorMessage<CommunicatorConnectPayload> msg);

    void onPublish(CommunicatorMessage<CommunicatorPublishPayload> msg);

    void onPubAck(CommunicatorMessage<CommunicatorPacketIdPayload> msg);

    void onPubRec(CommunicatorMessage<CommunicatorPacketIdPayload> msg);

    void onPubRel(CommunicatorMessage<CommunicatorPacketIdPayload> msg);

    void onPubComp(CommunicatorMessage<CommunicatorPacketIdPayload> msg);

    void onSubscribe(CommunicatorMessage<CommunicatorSubscribePayload> msg);

    void onUnsubscribe(CommunicatorMessage<CommunicatorUnsubscribePayload> msg);

    void onDisconnect(CommunicatorMessage msg);
}
