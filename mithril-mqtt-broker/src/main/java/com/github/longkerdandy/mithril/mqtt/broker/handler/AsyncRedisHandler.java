package com.github.longkerdandy.mithril.mqtt.broker.handler;

import com.github.longkerdandy.mithril.mqtt.api.Authenticator;
import com.github.longkerdandy.mithril.mqtt.api.AuthorizeResult;
import com.github.longkerdandy.mithril.mqtt.api.Communicator;
import com.github.longkerdandy.mithril.mqtt.broker.session.SessionRegistry;
import com.github.longkerdandy.mithril.mqtt.broker.util.Validator;
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
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.longkerdandy.mithril.mqtt.storage.redis.RedisStorage.mapToMqtt;
import static com.github.longkerdandy.mithril.mqtt.util.UuidUtils.shortUuid;

/**
 * Asynchronous MQTT Handler using Redis
 */
public class AsyncRedisHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private static final Logger logger = LoggerFactory.getLogger(AsyncRedisHandler.class);

    protected final Authenticator authenticator;
    protected final Communicator communicator;
    protected final RedisStorage redis;
    protected final SessionRegistry registry;
    protected final PropertiesConfiguration config;
    protected final Validator validator;

    // session state
    protected String clientId;
    protected String userName;
    protected boolean connected;
    protected boolean cleanSession;
    protected int keepAlive;
    protected MqttPublishMessage willMessage;

    public AsyncRedisHandler(Authenticator authenticator, Communicator communicator, RedisStorage redis, SessionRegistry registry, PropertiesConfiguration config, Validator validator) {
        this.authenticator = authenticator;
        this.communicator = communicator;
        this.redis = redis;
        this.registry = registry;
        this.config = config;
        this.validator = validator;
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
        this.clientId = msg.payload().clientIdentifier();
        this.cleanSession = msg.variableHeader().isCleanSession();

        // A Server MAY allow a Client to supply a ClientId that has a length of zero bytes, however if it does so the
        // Server MUST treat this as a special case and assign a unique ClientId to that Client. It MUST then
        // process the CONNECT packet as if the Client had provided that unique ClientId
        // If the Client supplies a zero-byte ClientId with CleanSession set to 0, the Server MUST respond to the
        // CONNECT Packet with a CONNACK return code 0x02 (Identifier rejected) and then close the Network
        // Connection
        if (StringUtils.isBlank(this.clientId)) {
            this.clientId = shortUuid();
            if (!this.cleanSession) {
                logger.trace("Protocol violation: Empty client id with clean session set to 0, send CONNACK and disconnect the client");
                this.registry.sendMessage(
                        ctx,
                        MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED),
                                null),
                        this.clientId,
                        null,
                        true);
                ctx.close();
                return;
            }
        }

        // Validate clientId based on configuration
        if (!validator.isClientIdValid(this.clientId)) {
            logger.trace("Protocol violation: Client id {} not valid based on configuration, send CONNACK and disconnect the client");
            this.registry.sendMessage(
                    ctx,
                    MqttMessageFactory.newMessage(
                            new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                            new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED),
                            null),
                    this.clientId,
                    null,
                    true);
            ctx.close();
            return;
        }

        // A Client can only send the CONNECT Packet once over a Network Connection. The Server MUST
        // process a second CONNECT Packet sent from a Client as a protocol violation and disconnect the Client
        if (this.connected) {
            logger.trace("Protocol violation: Second CONNECT packet sent from client {}, disconnect the client", this.clientId);
            ctx.close();
            return;
        }

        boolean userNameFlag = msg.variableHeader().hasUserName();
        boolean passwordFlag = msg.variableHeader().hasPassword();
        this.userName = msg.payload().userName();
        String password = msg.payload().password();
        boolean malformed = false;
        // If the User Name Flag is set to 0, a user name MUST NOT be present in the payload
        // If the User Name Flag is set to 1, a user name MUST be present in the payload
        // If the Password Flag is set to 0, a password MUST NOT be present in the payload
        // If the Password Flag is set to 1, a password MUST be present in the payload
        // If the User Name Flag is set to 0, the Password Flag MUST be set to 0
        if (userNameFlag) {
            if (StringUtils.isBlank(this.userName)) malformed = true;
            if (passwordFlag && StringUtils.isBlank(password)) malformed = true;
            if (!passwordFlag && StringUtils.isNotBlank(password)) malformed = true;
        } else {
            if (StringUtils.isNotBlank(this.userName)) malformed = true;
            if (passwordFlag || StringUtils.isNotBlank(password)) malformed = true;
        }
        if (malformed) {
            logger.trace("Protocol violation: Bad user name or password from client {}, send CONNACK and disconnect the client", this.clientId);
            this.registry.sendMessage(
                    ctx,
                    MqttMessageFactory.newMessage(
                            new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                            new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD),
                            null),
                    this.clientId,
                    null,
                    true);
            ctx.close();
            return;
        }

        // Authorize client connect using provided Authenticator
        this.authenticator.authConnect(this.clientId, this.userName, password).thenAccept(result -> {

            // Authorize successful
            if (result == AuthorizeResult.OK) {
                logger.trace("Authorization Success: For client {}", this.clientId);

                // If the Server accepts a connection with CleanSession set to 1, the Server MUST set Session Present to 0
                // in the CONNACK packet in addition to setting a zero return code in the CONNACK packet
                // If the Server accepts a connection with CleanSession set to 0, the value set in Session Present depends
                // on whether the Server already has stored Session state for the supplied client ID. If the Server has stored
                // Session state, it MUST set Session Present to 1 in the CONNACK packet. If the Server
                // does not have stored Session state, it MUST set Session Present to 0 in the CONNACK packet. This is in
                // addition to setting a zero return code in the CONNACK packet.
                this.redis.isClientExist(this.clientId).thenAccept(exist -> {
                    boolean sessionPresent = false;
                    if (exist == 1 && !this.cleanSession) sessionPresent = true;

                    // The first packet sent from the Server to the Client MUST be a CONNACK Packet
                    logger.trace("Connection Accepted: Send CONNACK back to client {}", this.clientId);
                    // TODO: CONNACK message should contain sessionPresent
                    this.registry.sendMessage(
                            ctx,
                            MqttMessageFactory.newMessage(
                                    new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                    new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED),
                                    null),
                            this.clientId,
                            null,
                            true);

                    // If the ClientId represents a Client already connected to the Server then the Server MUST
                    // disconnect the existing Client
                    this.redis.getConnectedNodes(this.clientId).thenAccept(nodes -> {
                        // Disconnect local node
                        if (nodes.contains(this.config.getString("node.id"))) {
                            nodes.remove(this.config.getString("node.id"));
                            ChannelHandlerContext lastSession = this.registry.getSession(this.clientId);
                            if (lastSession != null) {
                                logger.trace("Disconnect Existed: Disconnect existed client {} from local node {}", this.clientId, this.config.getString("node.id"));
                                lastSession.close();
                                this.registry.removeSession(this.clientId, lastSession);
                            }
                        }
                        // Disconnect remote nodes
                        this.communicator.oneToMany(nodes,
                                MqttMessageFactory.newMessage(
                                        new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                        null,
                                        null
                                ),
                                new HashMap<String, String>() {{
                                    put("clientId", clientId);
                                }});
                    });

                    // If CleanSession is set to 0, the Server MUST resume communications with the Client based on state from
                    // the current Session (as identified by the Client identifier). If there is no Session associated with the Client
                    // identifier the Server MUST create a new Session. The Client and Server MUST store the Session after
                    // the Client and Server are disconnected. After the disconnection of a Session that had
                    // CleanSession set to 0, the Server MUST store further QoS 1 and QoS 2 messages that match any
                    // subscriptions that the client had at the time of disconnection as part of the Session state.
                    // It MAY also store QoS 0 messages that meet the same criteria.
                    // The Session state in the Server consists of:
                    // The existence of a Session, even if the rest of the Session state is empty.
                    // The Client's subscriptions.
                    // QoS 1 and QoS 2 messages which have been sent to the Client, but have not been completely acknowledged.
                    // QoS 1 and QoS 2 messages pending transmission to the Client.
                    // QoS 2 messages which have been received from the Client, but have not been completely acknowledged.
                    // Optionally, QoS 0 messages pending transmission to the Client.
                    if (!this.cleanSession) {
                        if (exist == 1) {
                            // Only care about messages in the session with clean session = 0
                            this.redis.getInFlightIds(this.clientId, false).thenAccept(ids -> {
                                for (String packetId : ids) {
                                    this.redis.getInFlightMessage(this.clientId, Integer.parseInt(packetId)).thenAccept(map -> {
                                        MqttMessage mqtt = mapToMqtt(map);
                                        this.registry.sendMessage(ctx, mqtt, this.clientId, Integer.parseInt(map.getOrDefault("packetId", "0")), false);
                                    });
                                }
                                ctx.flush();
                            });
                        }
                    }
                    // If CleanSession is set to 1, the Client and Server MUST discard any previous Session and start a new
                    // one. This Session lasts as long as the Network Connection. State data associated with this Session
                    // MUST NOT be reused in any subsequent Session.
                    // When CleanSession is set to 1 the Client and Server need not process the deletion of state atomically.
                    else {
                        if (exist == 1) {
                            // Only care about messages in the session with clean session = 0
                            this.redis.getInFlightIds(this.clientId, false).thenAccept(ids -> {
                                List<String> keys = new ArrayList<>();
                                keys.add(RedisKey.clientExist(this.clientId));
                                keys.add(RedisKey.inFlightList(this.clientId, false));
                                keys.addAll(ids.stream().map(packetId -> RedisKey.inFlightMessage(this.clientId, Integer.parseInt(packetId))).collect(Collectors.toList()));
                                this.redis.removeKeys(keys.toArray(new String[keys.size()]));
                            });
                            this.redis.removeAllSubscriptions(this.clientId, false);
                        }
                    }

                    // If the Will Flag is set to 1 this indicates that, if the Connect request is accepted, a Will Message MUST be
                    // stored on the Server and associated with the Network Connection. The Will Message MUST be published
                    // when the Network Connection is subsequently closed unless the Will Message has been deleted by the
                    // Server on receipt of a DISCONNECT Packet.
                    // Situations in which the Will Message is published include, but are not limited to:
                    // An I/O error or network failure detected by the Server.
                    // The Client fails to communicate within the Keep Alive time.
                    // The Client closes the Network Connection without first sending a DISCONNECT Packet.
                    // The Server closes the Network Connection because of a protocol error.
                    if (msg.variableHeader().isWillFlag()
                            && StringUtils.isNotBlank(msg.payload().willTopic())
                            && StringUtils.isNotBlank(msg.payload().willMessage())) {
                        MqttQoS willQos = MqttQoS.valueOf(msg.variableHeader().willQos());
                        boolean willRetain = msg.variableHeader().isWillRetain();
                        this.willMessage = (MqttPublishMessage) MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.PUBLISH, false, willQos, willRetain, 0),
                                new MqttPublishVariableHeader(msg.payload().willTopic(), 0),
                                null
                        );
                    }

                    // If the Keep Alive value is non-zero and the Server does not receive a Control Packet from the Client
                    // within one and a half times the Keep Alive time period, it MUST disconnect the Network Connection to the
                    // Client as if the network had failed
                    this.keepAlive = msg.variableHeader().keepAliveTimeSeconds();
                    if (this.keepAlive <= 0 || this.keepAlive > this.config.getInt("mqtt.keepalive.max"))
                        this.keepAlive = this.config.getInt("mqtt.keepalive.default");

                    // Always clear data in the session with clean session = 1
                    this.redis.removeAllSubscriptions(this.clientId, true);
                    this.redis.getInFlightIds(this.clientId, true).thenAccept(ids -> {
                        List<String> keys = new ArrayList<>();
                        keys.add(RedisKey.inFlightList(this.clientId, true));
                        keys.addAll(ids.stream().map(packetId -> RedisKey.inFlightMessage(this.clientId, Integer.parseInt(packetId))).collect(Collectors.toList()));
                        this.redis.removeKeys(keys.toArray(new String[keys.size()]));
                    });

                    // Save connection state, add to local registry and remote storage
                    this.connected = true;
                    this.registry.saveSession(this.clientId, ctx);
                    this.redis.updateConnectedNodes(this.clientId, this.config.getString("node.id"));
                    if (exist != 1 && !cleanSession) this.redis.setClientExist(this.clientId);
                });
            }

            // Authorize failed
            else {
                logger.trace("Authorization failed: CONNECT authorize {} for client {}, send CONNACK and disconnect the client", result, this.clientId);
                this.registry.sendMessage(
                        ctx,
                        MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED),
                                null),
                        this.clientId,
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
