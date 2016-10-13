package com.github.longkerdandy.mithqtt.api.message;

import com.github.longkerdandy.mithqtt.util.JSONs;
import io.netty.handler.codec.mqtt.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

/**
 * Message Entity Test
 */
public class MessageTest {

    @Test
    public void serializationTest() throws IOException {
        Message<MqttPacketIdVariableHeader, MqttSubscribePayloadGranted> msg = new Message<>(
                new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttAdditionalHeader(MqttVersion.MQTT_3_1_1, "Test_Client", "Test_User", "Test_Broker"),
                MqttPacketIdVariableHeader.from(12345),
                new MqttSubscribePayloadGranted(Collections.singletonList(new MqttTopicSubscriptionGranted("abc/+/g/h", MqttGrantedQoS.AT_MOST_ONCE)))
        );
        byte[] bytes = JSONs.Mapper.writeValueAsBytes(msg);
        Message m = JSONs.decodeMessage(bytes);
        assert m != null;
        assert m.fixedHeader() != null;
        assert m.fixedHeader.messageType() == MqttMessageType.SUBSCRIBE;
        assert !m.fixedHeader.dup();
        assert m.fixedHeader.qos() == MqttQoS.AT_MOST_ONCE;
        assert !m.fixedHeader.retain();
        assert m.additionalHeader() != null;
        assert m.additionalHeader().version() == MqttVersion.MQTT_3_1_1;
        assert m.additionalHeader().clientId().equals("Test_Client");
        assert m.additionalHeader().userName().equals("Test_User");
        assert m.additionalHeader().brokerId().equals("Test_Broker");
        assert m.variableHeader() != null;
        assert ((MqttPacketIdVariableHeader) m.variableHeader()).packetId() == 12345;
        assert m.payload() != null;
        assert ((MqttSubscribePayloadGranted) m.payload()).subscriptions() != null;
        assert ((MqttSubscribePayloadGranted) m.payload()).subscriptions().size() == 1;
        assert ((MqttSubscribePayloadGranted) m.payload()).subscriptions().get(0).topic().equals("abc/+/g/h");
        assert ((MqttSubscribePayloadGranted) m.payload()).subscriptions().get(0).grantedQos() == MqttGrantedQoS.AT_MOST_ONCE;
    }
}
