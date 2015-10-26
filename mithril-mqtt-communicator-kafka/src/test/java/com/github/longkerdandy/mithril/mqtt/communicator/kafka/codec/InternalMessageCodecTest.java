package com.github.longkerdandy.mithril.mqtt.communicator.kafka.codec;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.api.internal.SubAck;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckReturnCode;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * InternalMessageDecoder Test
 */
public class InternalMessageCodecTest {

    @Test
    @SuppressWarnings("unchecked")
    public void subAckTest() {
        List<MqttSubAckReturnCode> grantedQosLevels = new ArrayList<>();
        grantedQosLevels.add(MqttSubAckReturnCode.SUCCESS_MAX_QOS2);
        grantedQosLevels.add(MqttSubAckReturnCode.FAILURE);
        grantedQosLevels.add(MqttSubAckReturnCode.SUCCESS_MAX_QOS1);
        SubAck subAck = new SubAck(10000, grantedQosLevels);
        InternalMessage<SubAck> msg = new InternalMessage<>(MqttMessageType.SUBACK, false, MqttQoS.AT_LEAST_ONCE, false,
                MqttVersion.MQTT_3_1_1, "Client_A", "User_A", "Broker_A", subAck);

        InternalMessageDecoder decoder = new InternalMessageDecoder();
        InternalMessageEncoder encoder = new InternalMessageEncoder();

        // encode
        byte[] bytes = encoder.toBytes(msg);
        assert bytes != null && bytes.length > 0;

        // decode
        msg = decoder.fromBytes(bytes);
        assert msg.getMessageType() == MqttMessageType.SUBACK;
        assert !msg.isDup();
        assert msg.getQos() == MqttQoS.AT_LEAST_ONCE;
        assert !msg.isRetain();
        assert msg.getVersion() == MqttVersion.MQTT_3_1_1;
        assert msg.getClientId().equals("Client_A");
        assert msg.getUserName().equals("User_A");
        assert msg.getBrokerId().equals("Broker_A");
        assert msg.getPayload().getPacketId() == 10000;
        assert msg.getPayload().getGrantedQoSLevels().size() == 3;
        assert msg.getPayload().getGrantedQoSLevels().get(0) == MqttSubAckReturnCode.SUCCESS_MAX_QOS2;
        assert msg.getPayload().getGrantedQoSLevels().get(1) == MqttSubAckReturnCode.FAILURE;
        assert msg.getPayload().getGrantedQoSLevels().get(2) == MqttSubAckReturnCode.SUCCESS_MAX_QOS1;
    }
}
