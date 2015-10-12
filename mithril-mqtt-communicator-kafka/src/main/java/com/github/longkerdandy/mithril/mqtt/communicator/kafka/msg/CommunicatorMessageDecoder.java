package com.github.longkerdandy.mithril.mqtt.communicator.kafka.msg;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.longkerdandy.mithril.mqtt.api.comm.*;
import kafka.serializer.Decoder;

import java.io.IOException;

import static com.github.longkerdandy.mithril.mqtt.communicator.kafka.util.JSONs.Mapper;

/**
 * CommunicatorMessage Kafka Decoder
 */
public class CommunicatorMessageDecoder implements Decoder<CommunicatorMessage> {

    @Override
    public CommunicatorMessage fromBytes(byte[] bytes) {
        try {
            JavaType type = Mapper.getTypeFactory().constructParametrizedType(CommunicatorMessage.class, CommunicatorMessage.class, JsonNode.class);
            CommunicatorMessage<JsonNode> m = Mapper.readValue(bytes, type);
            switch (m.getMessageType()) {
                case CONNECT:
                    CommunicatorConnectPayload connect = Mapper.treeToValue(m.getPayload(), CommunicatorConnectPayload.class);
                    CommunicatorMessage<CommunicatorConnectPayload> mc = new CommunicatorMessage<>();
                    mc.cloneFields(m);
                    mc.setPayload(connect);
                    return mc;
                case CONNACK:
                    CommunicatorConnAckPayload connack = Mapper.treeToValue(m.getPayload(), CommunicatorConnAckPayload.class);
                    CommunicatorMessage<CommunicatorConnAckPayload> mca = new CommunicatorMessage<>();
                    mca.cloneFields(m);
                    mca.setPayload(connack);
                    return mca;
                case SUBSCRIBE:
                    CommunicatorSubscribePayload subscribe = Mapper.treeToValue(m.getPayload(), CommunicatorSubscribePayload.class);
                    CommunicatorMessage<CommunicatorSubscribePayload> ms = new CommunicatorMessage<>();
                    ms.cloneFields(m);
                    ms.setPayload(subscribe);
                    return ms;
                case SUBACK:
                    CommunicatorSubAckPayload suback = Mapper.treeToValue(m.getPayload(), CommunicatorSubAckPayload.class);
                    CommunicatorMessage<CommunicatorSubAckPayload> msa = new CommunicatorMessage<>();
                    msa.cloneFields(m);
                    msa.setPayload(suback);
                    return msa;
                case UNSUBSCRIBE:
                    CommunicatorUnsubscribePayload unsubscribe = Mapper.treeToValue(m.getPayload(), CommunicatorUnsubscribePayload.class);
                    CommunicatorMessage<CommunicatorUnsubscribePayload> mu = new CommunicatorMessage<>();
                    mu.cloneFields(m);
                    mu.setPayload(unsubscribe);
                    return mu;
                case PUBLISH:
                    CommunicatorPublishPayload publish = Mapper.treeToValue(m.getPayload(), CommunicatorPublishPayload.class);
                    CommunicatorMessage<CommunicatorPublishPayload> mp = new CommunicatorMessage<>();
                    mp.cloneFields(m);
                    mp.setPayload(publish);
                    return mp;
                case UNSUBACK:
                case PUBACK:
                case PUBREC:
                case PUBREL:
                case PUBCOMP:
                    CommunicatorPacketIdPayload packetId = Mapper.treeToValue(m.getPayload(), CommunicatorPacketIdPayload.class);
                    CommunicatorMessage<CommunicatorPacketIdPayload> mpi = new CommunicatorMessage<>();
                    mpi.cloneFields(m);
                    mpi.setPayload(packetId);
                    return mpi;
                case PINGREQ:
                case PINGRESP:
                case DISCONNECT:
                    return m;
                default:
                    return null;
            }
        } catch (IOException e) {
            return null;
        }
    }
}
