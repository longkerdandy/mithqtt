package com.github.longkerdandy.mithqtt.broker.handler;

import com.github.longkerdandy.mithqtt.api.metrics.MessageDirection;
import com.github.longkerdandy.mithqtt.api.metrics.MetricsService;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.apache.commons.lang3.StringUtils;

/**
 * Metrics Handler based on Message
 */
public class MessageMetricsHandler extends ChannelDuplexHandler {

    protected final MetricsService metrics;
    protected final String brokerId;
    protected String clientId;

    public MessageMetricsHandler(MetricsService metrics, String brokerId) {
        this.metrics = metrics;
        this.brokerId = brokerId;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof MqttMessage) {
            MqttMessage mqtt = (MqttMessage) msg;
            if (StringUtils.isBlank(this.clientId) && mqtt.fixedHeader().messageType() == MqttMessageType.CONNECT) {
                this.clientId = ((MqttConnectPayload) mqtt.payload()).clientId();
            }
            if (StringUtils.isNotBlank(this.clientId)) {
                this.metrics.measurement(this.clientId, this.brokerId, MessageDirection.IN, mqtt.fixedHeader().messageType());
            }
            this.metrics.measurement(this.brokerId, MessageDirection.IN, mqtt.fixedHeader().messageType());
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof MqttMessage) {
            MqttMessage mqtt = (MqttMessage) msg;
            if (StringUtils.isNotBlank(this.clientId)) {
                this.metrics.measurement(this.clientId, this.brokerId, MessageDirection.OUT, mqtt.fixedHeader().messageType());
            }
            this.metrics.measurement(this.brokerId, MessageDirection.OUT, mqtt.fixedHeader().messageType());
        }
        ctx.write(msg, promise);
    }
}
