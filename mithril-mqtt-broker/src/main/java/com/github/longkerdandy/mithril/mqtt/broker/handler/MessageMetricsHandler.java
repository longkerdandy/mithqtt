package com.github.longkerdandy.mithril.mqtt.broker.handler;

import com.codahale.metrics.MetricRegistry;
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

    protected final String brokerId;
    protected final MetricRegistry registry;

    private String clientId;

    public MessageMetricsHandler(String brokerId, MetricRegistry registry) {
        this.brokerId = brokerId;
        this.registry = registry;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof MqttMessage) {
            MqttMessage mqtt = (MqttMessage) msg;
            if (this.clientId == null && mqtt.fixedHeader().messageType() == MqttMessageType.CONNECT) {
                this.clientId = ((MqttConnectPayload) mqtt.payload()).clientId();
            }
            if (this.clientId != null) {
                this.registry.counter("mqtt.clint." + this.clientId + ".in").inc();
            }
            this.registry.counter("mqtt.broker." + this.brokerId + ".in").inc();
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof MqttMessage) {
            if (this.clientId != null) {
                this.registry.counter("mqtt.clint." + this.clientId + ".out").inc();
            }
            this.registry.counter("mqtt.broker." + this.brokerId + ".out").inc();
        }
        ctx.write(msg, promise);
    }
}
