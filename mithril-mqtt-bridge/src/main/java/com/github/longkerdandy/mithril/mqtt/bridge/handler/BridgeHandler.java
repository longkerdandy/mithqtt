package com.github.longkerdandy.mithril.mqtt.bridge.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;

/**
 * MQTT Bridge Handler
 */
public class BridgeHandler extends SimpleChannelInboundHandler<MqttMessage> {

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        switch (msg.fixedHeader().messageType()) {
            case CONNECT:
                onConnect(ctx, (MqttConnectMessage) msg);
                break;
            case PUBLISH:
                onPublish(ctx, (MqttPublishMessage) msg);
                break;
            case PUBACK:
                onPubAck(ctx, msg);
                break;
            case PUBREC:
                onPubRec(ctx, msg);
                break;
            case PUBREL:
                onPubRel(ctx, msg);
                break;
            case PUBCOMP:
                onPubComp(ctx, msg);
                break;
            case SUBSCRIBE:
                onSubscribe(ctx, (MqttSubscribeMessage) msg);
                break;
            case UNSUBSCRIBE:
                onSubAck(ctx, (MqttSubAckMessage) msg);
                break;
            case PINGREQ:
                onPingReq(ctx, msg);
                break;
            case DISCONNECT:
                onDisconnect(ctx, msg);
                break;
        }
    }

    protected void onConnect(ChannelHandlerContext ctx, MqttConnectMessage msg) {

    }

    protected void onPublish(ChannelHandlerContext ctx, MqttPublishMessage msg) {

    }

    protected void onPubAck(ChannelHandlerContext ctx, MqttMessage msg) {

    }

    protected void onPubRec(ChannelHandlerContext ctx, MqttMessage msg) {

    }

    protected void onPubRel(ChannelHandlerContext ctx, MqttMessage msg) {

    }

    protected void onPubComp(ChannelHandlerContext ctx, MqttMessage msg) {

    }

    protected void onSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage msg) {

    }

    protected void onSubAck(ChannelHandlerContext ctx, MqttSubAckMessage msg) {

    }

    protected void onPingReq(ChannelHandlerContext ctx, MqttMessage msg) {

    }

    protected void onDisconnect(ChannelHandlerContext ctx, MqttMessage msg) {

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}
