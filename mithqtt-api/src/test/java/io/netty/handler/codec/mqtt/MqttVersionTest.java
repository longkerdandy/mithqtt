package io.netty.handler.codec.mqtt;

import org.junit.Test;

/**
 * Mqtt Version Test
 */
public class MqttVersionTest {

    @Test
    public void test() {
        int l1 = 3;
        int l2 = 4;
        assert MqttVersion.fromProtocolNameAndLevel("MQIsdp", (byte) l1) == MqttVersion.MQTT_3_1;
        assert MqttVersion.fromProtocolNameAndLevel("MQTT", (byte) l2) == MqttVersion.MQTT_3_1_1;
    }
}
