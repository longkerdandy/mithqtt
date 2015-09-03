package com.github.longkerdandy.mithril.mqtt.broker.handler;

import com.github.longkerdandy.mithril.mqtt.api.Authenticator;
import com.github.longkerdandy.mithril.mqtt.api.Communicator;
import com.github.longkerdandy.mithril.mqtt.broker.session.SessionRegistry;
import com.github.longkerdandy.mithril.mqtt.entity.InFlightMessage;
import com.github.longkerdandy.mithril.mqtt.storage.redis.RedisKey;
import com.github.longkerdandy.mithril.mqtt.storage.redis.RedisStorage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.longkerdandy.mithril.mqtt.broker.util.Converter.inFlightToMqtt;
import static com.github.longkerdandy.mithril.mqtt.storage.redis.RedisStorage.toInFlight;

/**
 * Asynchronous MQTT Handler using Redis
 * This handler will invoke blocking methods, should be executed on a different EventLoop
 */
public class AsyncRedisHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private static final Logger logger = LoggerFactory.getLogger(AsyncRedisHandler.class);

    protected final Authenticator authenticator;
    protected final Communicator communicator;
    protected final RedisStorage redis;
    protected final SessionRegistry registry;
    protected final PropertiesConfiguration config;

    // states
    protected boolean connected;
    protected boolean cleanSession;

    public AsyncRedisHandler(Authenticator authenticator, Communicator communicator, RedisStorage redis, SessionRegistry registry, PropertiesConfiguration config) {
        this.authenticator = authenticator;
        this.communicator = communicator;
        this.redis = redis;
        this.registry = registry;
        this.config = config;
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        // Disconnect if The MQTT message is invalid
        if (msg.decoderResult().isFailure()) {
            Throwable cause = msg.decoderResult().cause();
            logger.trace("Invalid message: {}", ExceptionUtils.getMessage(msg.decoderResult().cause()));
            if (cause instanceof MqttUnacceptableProtocolVersionException) {
                // Send back CONNACK if the protocol version is invalid
                this.registry.sendMessage(
                        ctx,
                        MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION),
                                null),
                        "INVALID",
                        null,
                        true);
            } else if (cause instanceof MqttIdentifierRejectedException) {
                // Send back CONNACK if the client id is invalid
                this.registry.sendMessage(
                        ctx,
                        MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED),
                                null),
                        "INVALID",
                        null,
                        true);
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

        boolean userNameFlag = msg.variableHeader().hasUserName();
        boolean passwordFlag = msg.variableHeader().hasPassword();
        String userName = msg.payload().userName();
        String password = msg.payload().password();
        boolean malformed = false;
        // If the User Name Flag is set to 0, a user name MUST NOT be present in the payload
        // If the User Name Flag is set to 1, a user name MUST be present in the payload
        // If the Password Flag is set to 0, a password MUST NOT be present in the payload
        // If the Password Flag is set to 1, a password MUST be present in the payload
        // If the User Name Flag is set to 0, the Password Flag MUST be set to 0
        if (userNameFlag) {
            if (StringUtils.isBlank(userName)) malformed = true;
            if (passwordFlag && StringUtils.isBlank(password)) malformed = true;
            if (!passwordFlag && StringUtils.isNotBlank(password)) malformed = true;
        } else {
            if (StringUtils.isNotBlank(userName)) malformed = true;
            if (passwordFlag || StringUtils.isNotBlank(password)) malformed = true;
        }
        if (malformed) {
            logger.trace("Protocol violation: Bad user name or password from client {}, send CONNACK and disconnect the client", clientId);
            this.registry.sendMessage(
                    ctx,
                    MqttMessageFactory.newMessage(
                            new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                            new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD),
                            null),
                    clientId,
                    null,
                    true);
            ctx.close();
            return;
        }

        // Authorize client
        this.authenticator.authConnect(clientId, userName, password).thenAccept(integer -> {

            if (integer == Authenticator.AUTH_SUCCESS) {
                cleanSession = msg.variableHeader().isCleanSession();
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
                if (!cleanSession) {
                    redis.getInFlightIds(clientId).thenAccept(ids -> {
                        for (String packetId : ids) {
                            redis.getInFlightMessage(clientId, Integer.parseInt(packetId)).thenAccept(map -> {
                                InFlightMessage inFlight = toInFlight(map);
                                MqttMessage mqtt = inFlightToMqtt(inFlight);
                                registry.sendMessage(ctx, mqtt, clientId, inFlight.getPacketId(), false);
                            });
                        }
                        ctx.flush();
                    });
                }
                // If CleanSession is set to 1, the Client and Server MUST discard any previous Session and start a new
                // one. This Session lasts as long as the Network Connection. State data associated with this Session
                // MUST NOT be reused in any subsequent Session.
                // When CleanSession is set to 1 the Client and Server need not process the deletion of state atomically.
                else {
                    redis.removeSubscriptions(clientId);
                    redis.getInFlightIds(clientId).thenAccept(ids -> {
                        List<String> keys = new ArrayList<>();
                        keys.add(RedisKey.inFlightList(clientId));
                        keys.addAll(ids.stream().map(packetId -> RedisKey.inFlightMessage(clientId, Integer.parseInt(packetId))).collect(Collectors.toList()));
                        redis.removeKeys(keys.toArray(new String[keys.size()]));
                    });
                }

                // If the ClientId represents a Client already connected to the Server then the Server MUST
                // disconnect the existing Client

                connected = true;

            } else {

                logger.trace("Authorization failed: CONNECT authorize {} for client {}, send CONNACK and disconnect the client", integer, clientId);
                registry.sendMessage(
                        ctx,
                        MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED),
                                null),
                        clientId,
                        null,
                        true);
                ctx.close();

            }
        });
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
