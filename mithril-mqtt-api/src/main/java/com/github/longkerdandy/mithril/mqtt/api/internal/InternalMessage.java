package com.github.longkerdandy.mithril.mqtt.api.internal;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;

/**
 * Represent MQTT Message passed in Communicator
 */
public class InternalMessage<T> {

    // fixed header
    private MqttMessageType messageType;
    private boolean dup;
    private MqttQoS qos;
    private boolean retain;

    // some info in CONNECT message, but useful for stateless transfer
    private MqttVersion version;
    private boolean cleanSession;
    private String clientId;
    private String userName;

    // variable header and payload
    private T payload;

    public MqttMessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MqttMessageType messageType) {
        this.messageType = messageType;
    }

    public boolean isDup() {
        return dup;
    }

    public void setDup(boolean dup) {
        this.dup = dup;
    }

    public MqttQoS getQos() {
        return qos;
    }

    public void setQos(MqttQoS qos) {
        this.qos = qos;
    }

    public boolean isRetain() {
        return retain;
    }

    public void setRetain(boolean retain) {
        this.retain = retain;
    }

    public MqttVersion getVersion() {
        return version;
    }

    public void setVersion(MqttVersion version) {
        this.version = version;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

    public void cloneFields(InternalMessage msg) {
        this.messageType = msg.messageType;
        this.dup = msg.dup;
        this.qos = msg.qos;
        this.retain = msg.retain;
        this.version = msg.version;
        this.cleanSession = msg.cleanSession;
        this.clientId = msg.clientId;
        this.userName = msg.userName;
    }

    @SuppressWarnings("unchecked")
    public static InternalMessage fromMqttMessage(MqttVersion version, boolean cleanSession,
                                                  String clientId, String userName, MqttMessage mqtt) {
        InternalMessage msg = new InternalMessage();

        msg.version = version;
        msg.cleanSession = cleanSession;
        msg.clientId = clientId;
        msg.userName = userName;

        // fixed header
        msg.messageType = mqtt.fixedHeader().messageType();
        msg.dup = mqtt.fixedHeader().dup();
        msg.qos = mqtt.fixedHeader().qos();
        msg.retain = mqtt.fixedHeader().retain();

        switch (msg.messageType) {
            case CONNECT:
                Connect connect = new Connect();
                connect.setWillQos(((MqttConnectVariableHeader) mqtt.variableHeader()).willQos());
                connect.setWillRetain(((MqttConnectVariableHeader) mqtt.variableHeader()).willRetain());
                connect.setWillTopic(((MqttConnectPayload) mqtt.payload()).willTopic());
                // TODO: will message
                msg.setPayload(connect);
                break;
            case CONNACK:
                ConnAck connAck = new ConnAck();
                connAck.setSessionPresent(((MqttConnAckVariableHeader) mqtt.variableHeader()).sessionPresent());
                connAck.setConnectReturnCode(((MqttConnAckVariableHeader) mqtt.variableHeader()).connectReturnCode());
                msg.setPayload(connAck);
                break;
            case SUBSCRIBE:
                Subscribe subscribe = new Subscribe();
                subscribe.setPacketId(((MqttPacketIdVariableHeader) mqtt.variableHeader()).packetId());
                subscribe.setTopicSubscriptions(((MqttSubscribePayload) mqtt.payload()).topicSubscriptions());
                msg.setPayload(subscribe);
                break;
            case SUBACK:
                SubAck subAck = new SubAck();
                subAck.setPacketId(((MqttPacketIdVariableHeader) mqtt.variableHeader()).packetId());
                // TODO: granted QoS
                msg.setPayload(subAck);
                break;
            case PUBLISH:
                Publish publish = new Publish();
                publish.setPacketId(((MqttPublishVariableHeader) mqtt.variableHeader()).packetId());
                publish.setTopicName(((MqttPublishVariableHeader) mqtt.variableHeader()).topicName());
                ByteBuf buf = (ByteBuf) mqtt.variableHeader();
                byte[] bytes = new byte[buf.readableBytes()];
                buf.readBytes(bytes);
                publish.setPayload(bytes);
                msg.setPayload(publish);
                break;
            case UNSUBACK:
            case PUBACK:
            case PUBREC:
            case PUBREL:
            case PUBCOMP:
                PacketId packetId = new PacketId();
                packetId.setPacketId(((MqttPacketIdVariableHeader) mqtt.variableHeader()).packetId());
                msg.setPayload(packetId);
                break;
            // TODO: Disconnect
        }

        return msg;
    }
}
