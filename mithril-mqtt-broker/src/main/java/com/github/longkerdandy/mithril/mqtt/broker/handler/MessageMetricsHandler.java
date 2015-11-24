package com.github.longkerdandy.mithril.mqtt.broker.handler;

import com.codahale.metrics.MetricRegistry;
import io.netty.channel.*;
import io.netty.handler.codec.mqtt.*;

/**
 * Metrics Handler based on Message
 */
public class MessageMetricsHandler extends ChannelDuplexHandler {

    protected final MetricRegistry registry;

    protected String clientId;

    public MessageMetricsHandler(MetricRegistry registry) {
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
                this.registry.meter("mqtt.clint." + this.clientId).mark();
                switch (mqtt.fixedHeader().messageType()) {
                    case CONNECT:
                        this.registry.meter("mqtt.clint." + this.clientId + ".connect").mark();
                        break;
                    case PUBLISH:
                        this.registry.meter("mqtt.clint." + this.clientId + ".publish").mark();
                        break;
                    case PUBACK:
                        this.registry.meter("mqtt.clint." + this.clientId + ".puback").mark();
                        break;
                    case PUBREC:
                        this.registry.meter("mqtt.clint." + this.clientId + ".pubrec").mark();
                        break;
                    case PUBREL:
                        this.registry.meter("mqtt.clint." + this.clientId + ".pubrel").mark();
                        break;
                    case PUBCOMP:
                        this.registry.meter("mqtt.clint." + this.clientId + ".pubcomp").mark();
                        break;
                    case SUBSCRIBE:
                        this.registry.meter("mqtt.clint." + this.clientId + ".subscribe").mark();
                        break;
                    case UNSUBSCRIBE:
                        this.registry.meter("mqtt.clint." + this.clientId + ".unsubscribe").mark();
                        break;
                    case PINGREQ:
                        this.registry.meter("mqtt.clint." + this.clientId + ".pingreq").mark();
                        break;
                    case DISCONNECT:
                        this.registry.meter("mqtt.clint." + this.clientId + ".disconnect").mark();
                        break;
                }
            }
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        ctx.write(msg, promise);
    }
}
