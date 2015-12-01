package com.github.longkerdandy.mithril.mqtt.broker.handler;

import com.github.longkerdandy.mithril.mqtt.api.metrics.MessageDirection;
import com.github.longkerdandy.mithril.mqtt.api.metrics.MetricsService;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;

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
            if (this.clientId == null && mqtt.fixedHeader().messageType() == MqttMessageType.CONNECT) {
                this.clientId = ((MqttConnectPayload) mqtt.payload()).clientId();
            }
            if (this.clientId != null) {
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
            if (this.clientId != null) {
                this.metrics.measurement(this.clientId, this.brokerId, MessageDirection.OUT, mqtt.fixedHeader().messageType());
            }
            this.metrics.measurement(this.brokerId, MessageDirection.OUT, mqtt.fixedHeader().messageType());
        }
        ctx.write(msg, promise);
    }
}
