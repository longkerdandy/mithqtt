package com.github.longkerdandy.mithqtt.api.message;

import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.util.internal.StringUtil;
import org.apache.commons.lang3.ArrayUtils;

import java.util.List;

/**
 * Payload of the {@link MqttSubscribeMessage}
 * After subscription was granted
 */
public class MqttSubscribePayloadGranted {

    protected List<MqttTopicSubscriptionGranted> subscriptions;

    public MqttSubscribePayloadGranted(List<MqttTopicSubscriptionGranted> subscriptions) {
        this.subscriptions = subscriptions;
    }

    public List<MqttTopicSubscriptionGranted> subscriptions() {
        return subscriptions;
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this)
                + '['
                + "subscriptions=" + ArrayUtils.toString(subscriptions)
                + ']';
    }
}
