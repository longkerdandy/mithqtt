package com.github.longkerdandy.mithril.mqtt.broker.handler;

import com.github.longkerdandy.mithril.mqtt.api.Authenticator;
import com.github.longkerdandy.mithril.mqtt.api.Communicator;
import com.github.longkerdandy.mithril.mqtt.api.Coordinator;
import com.github.longkerdandy.mithril.mqtt.api.Storage;
import com.github.longkerdandy.mithril.mqtt.broker.session.SessionRegistry;
import com.github.longkerdandy.mithril.mqtt.entity.InFlightMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Synchronized MQTT Handler
 * This handler will invoke blocking methods, should be executed on a different EventLoop
 */
public class SyncHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private static final Logger logger = LoggerFactory.getLogger(SyncHandler.class);

    protected final Authenticator authenticator;
    protected final Communicator communicator;
    protected final Coordinator coordinator;
    protected final Storage storage;
    protected final SessionRegistry registry;
    protected final PropertiesConfiguration config;

    // states
    protected boolean connected;
    protected boolean cleanSession;

    public SyncHandler(Authenticator authenticator, Communicator communicator, Coordinator coordinator, Storage storage, SessionRegistry registry, PropertiesConfiguration config) {
        this.authenticator = authenticator;
        this.communicator = communicator;
        this.coordinator = coordinator;
        this.storage = storage;
        this.registry = registry;
        this.config = config;
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        // invalid mqtt message
        if (msg.decoderResult().isFailure()) {
            Throwable cause = msg.decoderResult().cause();
            logger.trace("Invalid message: {}", ExceptionUtils.getMessage(msg.decoderResult().cause()));
            if (cause instanceof MqttUnacceptableProtocolVersionException) {
                // invalid protocol version
                ctx.write(MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION),
                        null));
            } else if (cause instanceof MqttIdentifierRejectedException) {
                // invalid clientId
                ctx.write(MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED),
                        null));
            }
            ctx.close();
            return;
        }

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
        String clientId = msg.payload().clientIdentifier();

        // A Client can only send the CONNECT Packet once over a Network Connection. The Server MUST
        // process a second CONNECT Packet sent from a Client as a protocol violation and disconnect the Client
        if (this.connected) {
            logger.trace("Protocol violation: Second CONNECT packet sent from client {}, disconnect the client", clientId);
            ctx.close();
            return;
        }
        this.connected = true;

         this.cleanSession = msg.variableHeader().isCleanSession();
        // If CleanSession is set to 0, the Server MUST resume communications with the Client based on state from
        // the current Session (as identified by the Client identifier). If there is no Session associated with the Client
        // identifier the Server MUST create a new Session. The Client and Server MUST store the Session after
        // the Client and Server are disconnected. After the disconnection of a Session that had
        // CleanSession set to 0, the Server MUST store further QoS 1 and QoS 2 messages that match any
        // subscriptions that the client had at the time of disconnection as part of the Session state.
        // It MAY also store QoS 0 messages that meet the same criteria.
        // The Session state in the Server consists of:
        // The existence of a Session, even if the rest of the Session state is empty.
        // The Client¡¯s subscriptions.
        // QoS 1 and QoS 2 messages which have been sent to the Client, but have not been completely acknowledged.
        // QoS 1 and QoS 2 messages pending transmission to the Client.
        // QoS 2 messages which have been received from the Client, but have not been completely acknowledged.
        // Optionally, QoS 0 messages pending transmission to the Client.
        if (!this.cleanSession) {
            List<InFlightMessage> inFlightMessages = this.storage.getInFlightMessages(clientId);
        }
        // If CleanSession is set to 1, the Client and Server MUST discard any previous Session and start a new
        // one. This Session lasts as long as the Network Connection. State data associated with this Session
        // MUST NOT be reused in any subsequent Session.
        // When CleanSession is set to 1 the Client and Server need not process the deletion of state atomically.
        else {
            this.storage.removeSubscriptions(clientId);
            this.storage.removeInFlightMessages(clientId);
        }

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
