package com.github.longkerdandy.mithril.mqtt.communicator.kafka.codec;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.longkerdandy.mithril.mqtt.api.internal.*;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

import static com.github.longkerdandy.mithril.mqtt.communicator.kafka.util.JSONs.Mapper;

public class InternalMessageDeserializer implements Deserializer<InternalMessage> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to do
    }

    @Override
    public InternalMessage deserialize(String topic, byte[] data) {
        try {
            JavaType type = Mapper.getTypeFactory().constructParametrizedType(InternalMessage.class, InternalMessage.class, JsonNode.class);
            InternalMessage<JsonNode> m = Mapper.readValue(data, type);
            switch (m.getMessageType()) {
                case CONNECT:
                    Connect connect = Mapper.treeToValue(m.getPayload(), Connect.class);
                    InternalMessage<Connect> mc = new InternalMessage<>();
                    mc.cloneFields(m);
                    mc.setPayload(connect);
                    return mc;
                case CONNACK:
                    ConnAck connack = Mapper.treeToValue(m.getPayload(), ConnAck.class);
                    InternalMessage<ConnAck> mca = new InternalMessage<>();
                    mca.cloneFields(m);
                    mca.setPayload(connack);
                    return mca;
                case SUBSCRIBE:
                    Subscribe subscribe = Mapper.treeToValue(m.getPayload(), Subscribe.class);
                    InternalMessage<Subscribe> ms = new InternalMessage<>();
                    ms.cloneFields(m);
                    ms.setPayload(subscribe);
                    return ms;
                case SUBACK:
                    SubAck suback = Mapper.treeToValue(m.getPayload(), SubAck.class);
                    InternalMessage<SubAck> msa = new InternalMessage<>();
                    msa.cloneFields(m);
                    msa.setPayload(suback);
                    return msa;
                case UNSUBSCRIBE:
                    Unsubscribe unsubscribe = Mapper.treeToValue(m.getPayload(), Unsubscribe.class);
                    InternalMessage<Unsubscribe> mu = new InternalMessage<>();
                    mu.cloneFields(m);
                    mu.setPayload(unsubscribe);
                    return mu;
                case PUBLISH:
                    Publish publish = Mapper.treeToValue(m.getPayload(), Publish.class);
                    InternalMessage<Publish> mp = new InternalMessage<>();
                    mp.cloneFields(m);
                    mp.setPayload(publish);
                    return mp;
                case UNSUBACK:
                case PUBACK:
                case PUBREC:
                case PUBREL:
                case PUBCOMP:
                    PacketId packetId = Mapper.treeToValue(m.getPayload(), PacketId.class);
                    InternalMessage<PacketId> mpi = new InternalMessage<>();
                    mpi.cloneFields(m);
                    mpi.setPayload(packetId);
                    return mpi;
                case PINGREQ:
                case PINGRESP:
                case DISCONNECT:
                    return m;
                default:
                    throw new SerializationException("Error when deserializing byte[] to internal message due to unknown message type " + m.getMessageType());
            }
        } catch (IOException e) {
            throw new SerializationException("Error when deserializing byte[] to internal message", e);
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}
