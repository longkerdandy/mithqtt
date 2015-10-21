package com.github.longkerdandy.mithril.mqtt.broker.handler;

import com.github.longkerdandy.mithril.mqtt.api.auth.Authenticator;
import com.github.longkerdandy.mithril.mqtt.api.auth.AuthorizeResult;
import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.broker.session.SessionRegistry;
import com.github.longkerdandy.mithril.mqtt.broker.util.Validator;
import com.github.longkerdandy.mithril.mqtt.storage.redis.RedisStorage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.github.longkerdandy.mithril.mqtt.storage.redis.RedisStorage.mqttToMap;
import static com.github.longkerdandy.mithril.mqtt.util.UUIDs.shortUuid;

/**
 * Asynchronous MQTT Handler using Redis
 */
public class AsyncRedisHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private static final Logger logger = LoggerFactory.getLogger(AsyncRedisHandler.class);

    protected final Authenticator authenticator;
    protected final BrokerCommunicator communicator;
    protected final RedisStorage redis;
    protected final SessionRegistry registry;
    protected final PropertiesConfiguration config;
    protected final Validator validator;

    // session state
    protected MqttVersion version;
    protected String clientId;
    protected String userName;
    protected boolean connected;
    protected boolean cleanSession;
    protected int keepAlive;
    protected MqttPublishMessage willMessage;

    public AsyncRedisHandler(Authenticator authenticator, BrokerCommunicator communicator, RedisStorage redis, SessionRegistry registry, PropertiesConfiguration config, Validator validator) {
        this.authenticator = authenticator;
        this.communicator = communicator;
        this.redis = redis;
        this.registry = registry;
        this.config = config;
        this.validator = validator;
    }

    @Override
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        // Disconnect if The MQTT message is invalid
        if (msg.decoderResult().isFailure()) {
            Throwable cause = msg.decoderResult().cause();
            logger.debug("Protocol violation: Invalid message {}", ExceptionUtils.getMessage(msg.decoderResult().cause()));
            if (cause instanceof MqttUnacceptableProtocolVersionException) {
                // Send back CONNACK if the protocol version is invalid
                this.registry.sendMessage(
                        ctx,
                        MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, false),
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
                                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, false),
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
                onUnsubscribe(ctx, (MqttUnsubscribeMessage) msg);
                break;
            case PINGREQ:
                onPingReq(ctx);
                break;
            case DISCONNECT:
                onDisconnect(ctx);
                break;
        }
    }

    /**
     * Handle CONNECT MQTT Message
     *
     * @param ctx ChannelHandlerContext
     * @param msg CONNECT MQTT Message
     */
    protected void onConnect(ChannelHandlerContext ctx, MqttConnectMessage msg) {
        this.version = MqttVersion.fromProtocolNameAndLevel(msg.variableHeader().protocolName(), (byte) msg.variableHeader().protocolLevel());
        this.clientId = msg.payload().clientId();
        this.cleanSession = msg.variableHeader().cleanSession();

        // A Server MAY allow a Client to supply a ClientId that has a length of zero bytes, however if it does so the
        // Server MUST treat this as a special case and assign a unique ClientId to that Client. It MUST then
        // process the CONNECT packet as if the Client had provided that unique ClientId
        // If the Client supplies a zero-byte ClientId with CleanSession set to 0, the Server MUST respond to the
        // CONNECT Packet with a CONNACK return code 0x02 (Identifier rejected) and then close the Network
        // Connection
        if (StringUtils.isBlank(this.clientId)) {
            if (!this.cleanSession) {
                logger.debug("Protocol violation: Empty client id with clean session 0, send CONNACK and disconnect the client");
                this.registry.sendMessage(
                        ctx,
                        MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, false),
                                null),
                        "INVALID",
                        null,
                        true);
                ctx.close();
                return;
            }
            this.clientId = shortUuid();
        }

        // Validate clientId based on configuration
        if (!this.validator.isClientIdValid(this.clientId)) {
            logger.debug("Protocol violation: Client id {} not valid based on configuration, send CONNACK and disconnect the client", this.clientId);
            this.registry.sendMessage(
                    ctx,
                    MqttMessageFactory.newMessage(
                            new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                            new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, false),
                            null),
                    "INVALID",
                    null,
                    true);
            ctx.close();
            return;
        }

        // A Client can only send the CONNECT Packet once over a Network Connection. The Server MUST
        // process a second CONNECT Packet sent from a Client as a protocol violation and disconnect the Client
        if (this.connected) {
            logger.debug("Protocol violation: Second CONNECT packet sent from client {}, disconnect the client", this.clientId);
            ctx.close();
            return;
        }

        boolean userNameFlag = msg.variableHeader().userNameFlag();
        boolean passwordFlag = msg.variableHeader().passwordFlag();
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
            logger.debug("Protocol violation: Bad user name or password from client {}, send CONNACK and disconnect the client", this.clientId);
            this.registry.sendMessage(
                    ctx,
                    MqttMessageFactory.newMessage(
                            new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                            new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, false),
                            null),
                    this.clientId,
                    null,
                    true);
            ctx.close();
            return;
        }

        logger.debug("Message received: Received CONNECT message from client {} user {}", this.clientId, this.userName);

        // Authorize client connect using provided Authenticator
        this.authenticator.authConnectAsync(this.clientId, this.userName, password).thenAccept(result -> {

            // Authorize successful
            if (result == AuthorizeResult.OK) {
                logger.trace("Authorization succeed: Connection authorized for client {} user {}", this.clientId, this.userName);

                // If the Server accepts a connection with CleanSession set to 1, the Server MUST set Session Present to 0
                // in the CONNACK packet in addition to setting a zero return code in the CONNACK packet
                // If the Server accepts a connection with CleanSession set to 0, the value set in Session Present depends
                // on whether the Server already has stored Session state for the supplied client ID. If the Server has stored
                // Session state, it MUST set Session Present to 1 in the CONNACK packet. If the Server
                // does not have stored Session state, it MUST set Session Present to 0 in the CONNACK packet. This is in
                // addition to setting a zero return code in the CONNACK packet.
                this.redis.getSessionExist(this.clientId).thenAccept(exist -> {
                    boolean sessionPresent = "0".equals(exist) && !this.cleanSession;

                    // The first packet sent from the Server to the Client MUST be a CONNACK Packet
                    logger.trace("Message response: Send CONNACK back to client {}", this.clientId);
                    this.registry.sendMessage(
                            ctx,
                            MqttMessageFactory.newMessage(
                                    new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                    new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, sessionPresent),
                                    null),
                            this.clientId,
                            null,
                            true);

                    // If the ClientId represents a Client already connected to the Server then the Server MUST
                    // disconnect the existing Client
                    ChannelHandlerContext lastSession = this.registry.removeSession(this.clientId);
                    if (lastSession != null) {
                        logger.trace("Disconnect: Try to disconnect existed client {}", this.clientId);
                        lastSession.close();
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
                    // TODO: Deal with Will Message after Netty fixed the field type

                    // If the Keep Alive value is non-zero and the Server does not receive a Control Packet from the Client
                    // within one and a half times the Keep Alive time period, it MUST disconnect the Network Connection to the
                    // Client as if the network had failed
                    this.keepAlive = msg.variableHeader().keepAlive();
                    if (this.keepAlive <= 0 || this.keepAlive > this.config.getInt("mqtt.keepalive.max"))
                        this.keepAlive = this.config.getInt("mqtt.keepalive.default");

                    // Save connection state, add to local registry
                    this.connected = true;
                    this.registry.saveSession(this.clientId, ctx);

                    // Pass message to processor
                    this.communicator.sendToProcessor(InternalMessage.fromMqttMessage(this.version, this.cleanSession, this.clientId, this.userName, this.config.getString("broker.id"), msg));
                });
            }

            // Authorize failed
            else {
                logger.trace("Authorization failed: Connection unauthorized {} for client {}, send CONNACK and disconnect the client", result, this.clientId);
                this.registry.sendMessage(
                        ctx,
                        MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED, false),
                                null),
                        this.clientId,
                        null,
                        true);
                ctx.close();
            }
        });
    }

    /**
     * Handle PUBLISH MQTT Message
     *
     * @param ctx ChannelHandlerContext
     * @param msg CONNECT MQTT Message
     */
    protected void onPublish(ChannelHandlerContext ctx, MqttPublishMessage msg) {
        try {
            if (!this.connected) {
                logger.debug("Protocol violation: Client {} must first sent a CONNECT message, now received PUBLISH message, disconnect the client", this.clientId);
                ctx.close();
                return;
            }

            // boolean dup = msg.fixedHeader().dup();
            MqttQoS qos = msg.fixedHeader().qos();
            boolean retain = msg.fixedHeader().retain();
            String topicName = msg.variableHeader().topicName();
            int packetId = msg.variableHeader().packetId();

            // The Topic Name in the PUBLISH Packet MUST NOT contain wildcard characters
            if (!this.validator.isTopicNameValid(topicName)) {
                logger.debug("Protocol violation: Client {} sent PUBLISH message contains invalid topic name {}, disconnect the client", this.clientId, topicName);
                ctx.close();
                return;
            }

            // The Packet Identifier field is only present in PUBLISH Packets where the QoS level is 1 or 2.
            if (packetId <= 0 && (qos == MqttQoS.AT_LEAST_ONCE || qos == MqttQoS.EXACTLY_ONCE)) {
                logger.debug("Protocol violation: Client {} sent PUBLISH message does not contain packet id, disconnect the client", this.clientId);
                ctx.close();
                return;
            }

            logger.debug("Message received: Received PUBLISH message from client {} user {} topic {}", this.clientId, this.userName, topicName);

            // Authorize client publish using provided Authenticator
            this.authenticator.authPublishAsync(this.clientId, this.userName, topicName, qos.value(), retain).thenAccept(result -> {

                        // Authorize successful
                        if (result == AuthorizeResult.OK) {
                            logger.trace("Authorization succeed: Publish to topic {} authorized for client {}", topicName, this.clientId);

                            // Pass message to processor
                            this.communicator.sendToProcessor(InternalMessage.fromMqttMessage(this.version, this.cleanSession, this.clientId, this.userName, this.config.getString("broker.id"), msg));
                        }
                    }
            );

            // If a Server implementation does not authorize a PUBLISH to be performed by a Client; it has no way of
            // informing that Client. It MUST either make a positive acknowledgement, according to the normal QoS
            // rules, or close the Network Connection

            // In the QoS 1 delivery protocol, the Receiver
            // MUST respond with a PUBACK Packet containing the Packet Identifier from the incoming
            // PUBLISH Packet, having accepted ownership of the Application Message
            // The receiver is not required to complete delivery of the Application Message before sending the
            // PUBACK. When its original sender receives the PUBACK packet, ownership of the Application
            // Message is transferred to the receiver.
            if (qos == MqttQoS.AT_LEAST_ONCE) {
                logger.trace("Message response: Send PUBACK back to client {}", this.clientId);
                this.registry.sendMessage(
                        ctx,
                        MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                MqttPacketIdVariableHeader.from(packetId),
                                null),
                        this.clientId,
                        packetId,
                        true);
            }
            // In the QoS 2 delivery protocol, the Receiver
            // UST respond with a PUBREC containing the Packet Identifier from the incoming PUBLISH
            // Packet, having accepted ownership of the Application Message.
            // The receiver is not required to complete delivery of the Application Message before sending the
            // PUBREC or PUBCOMP. When its original sender receives the PUBREC packet, ownership of the
            // Application Message is transferred to the receiver.
            else if (qos == MqttQoS.EXACTLY_ONCE) {
                logger.trace("Message response: Send PUBREC back to client {}", this.clientId);
                this.registry.sendMessage(
                        ctx,
                        MqttMessageFactory.newMessage(
                                new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0),
                                MqttPacketIdVariableHeader.from(packetId),
                                null),
                        this.clientId,
                        packetId,
                        true);
            }
        } finally {
            // Always release ByteBuf
            msg.payload().release();
        }
    }

    protected void onPubAck(ChannelHandlerContext ctx, MqttMessage msg) {
        if (!this.connected) {
            logger.debug("Protocol violation: Client {} must first sent a CONNECT message, now received PUBACK message, disconnect the client", this.clientId);
            ctx.close();
            return;
        }

        logger.debug("Message received: Received PUBACK message from client {} user {}", this.clientId, this.userName);

        MqttPacketIdVariableHeader variable = (MqttPacketIdVariableHeader) msg.variableHeader();
        int packetId = variable.packetId();

        // In the QoS 1 delivery protocol, the Sender
        // MUST treat the PUBLISH Packet as “unacknowledged” until it has received the corresponding
        // PUBACK packet from the receiver.
        logger.trace("Remove in-flight: Remove in-flight PUBLISH message {} for client {}", packetId, this.clientId);
        this.redis.removeInFlightMessage(this.clientId, packetId);
    }

    protected void onPubRec(ChannelHandlerContext ctx, MqttMessage msg) {
        if (!this.connected) {
            logger.debug("Protocol violation: Client {} must first sent a CONNECT message, now received PUBREC message, disconnect the client", this.clientId);
            ctx.close();
            return;
        }

        logger.debug("Message received: Received PUBREC message from client {} user {}", this.clientId, this.userName);

        MqttPacketIdVariableHeader variable = (MqttPacketIdVariableHeader) msg.variableHeader();
        int packetId = variable.packetId();

        // In the QoS 2 delivery protocol, the Sender
        // MUST treat the PUBLISH packet as “unacknowledged” until it has received the corresponding
        // PUBREC packet from the receiver.
        // MUST send a PUBREL packet when it receives a PUBREC packet from the receiver. This
        // PUBREL packet MUST contain the same Packet Identifier as the original PUBLISH packet.
        // MUST NOT re-send the PUBLISH once it has sent the corresponding PUBREL packet.
        logger.trace("Remove in-flight: Remove in-flight PUBLISH message {} for client {}", packetId, this.clientId);
        this.redis.removeInFlightMessage(this.clientId, packetId).thenAccept(r -> {
            MqttMessage rel = MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                    MqttPacketIdVariableHeader.from(packetId),
                    null);
            logger.trace("Add in-flight: Add in-flight PUBREL message {} for client {}", packetId, this.clientId);
            this.redis.addInFlightMessage(this.clientId, packetId, mqttToMap(rel));
            logger.trace("Message response: Send PUBREL back to client {}", this.clientId);
            this.registry.sendMessage(ctx, rel, this.clientId, packetId, true);
        });
    }

    protected void onPubRel(ChannelHandlerContext ctx, MqttMessage msg) {
        if (!this.connected) {
            logger.debug("Protocol violation: Client {} must first sent a CONNECT message, now received PUBREL message, disconnect the client", this.clientId);
            ctx.close();
            return;
        }

        logger.debug("Message received: Received PUBREL message from client {} user {}", this.clientId, this.userName);

        MqttPacketIdVariableHeader variable = (MqttPacketIdVariableHeader) msg.variableHeader();
        int packetId = variable.packetId();

        // In the QoS 2 delivery protocol, the Receiver
        // MUST respond to a PUBREL packet by sending a PUBCOMP packet containing the same
        // Packet Identifier as the PUBREL.
        // After it has sent a PUBCOMP, the receiver MUST treat any subsequent PUBLISH packet that
        // contains that Packet Identifier as being a new publication.
        this.redis.removeQoS2MessageId(this.clientId, packetId);
        MqttMessage comp = MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttPacketIdVariableHeader.from(packetId),
                null);
        logger.trace("Message response: Send PUBCOMP back to client {}", this.clientId);
        this.registry.sendMessage(ctx, comp, this.clientId, packetId, true);
    }

    protected void onPubComp(ChannelHandlerContext ctx, MqttMessage msg) {
        if (!this.connected) {
            logger.debug("Protocol violation: Client {} must first sent a CONNECT message, now received PUBCOMP message, disconnect the client", this.clientId);
            ctx.close();
            return;
        }

        logger.debug("Message received: Received PUBCOMP message from client {} user {}", this.clientId, this.userName);

        MqttPacketIdVariableHeader variable = (MqttPacketIdVariableHeader) msg.variableHeader();
        int packetId = variable.packetId();

        // In the QoS 2 delivery protocol, the Sender
        // MUST treat the PUBREL packet as “unacknowledged” until it has received the corresponding
        // PUBCOMP packet from the receiver.
        logger.trace("Remove in-flight: Remove in-flight PUBREL message {} for client {}", packetId, this.clientId);
        this.redis.removeInFlightMessage(this.clientId, packetId);
    }

    protected void onSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage msg) {
        if (!this.connected) {
            logger.debug("Protocol violation: Client {} must first sent a CONNECT message, now received SUBSCRIBE message, disconnect the client", this.clientId);
            ctx.close();
            return;
        }

        int packetId = msg.variableHeader().packetId();
        List<MqttTopicSubscription> requestSubscriptions = msg.payload().subscriptions();

        logger.debug("Message received: Received SUBSCRIBE message from client {} user {}", this.clientId, this.userName);

        // Authorize client subscribe using provided Authenticator
        this.authenticator.authSubscribeAsync(this.clientId, this.userName, requestSubscriptions).thenAccept(grantedQosLevels -> {
            logger.trace("Authorization succeed: Subscribe to topic {} authorized for client {}", ArrayUtils.toString(msg.payload().subscriptions()), this.clientId);

            // Pass message to processor
            this.communicator.sendToProcessor(InternalMessage.fromMqttMessage(this.version, this.cleanSession, this.clientId, this.userName, this.config.getString("broker.id"), msg, grantedQosLevels));

            // If a Server receives a SUBSCRIBE packet that contains multiple Topic Filters it MUST handle that packet
            // as if it had received a sequence of multiple SUBSCRIBE packets, except that it combines their responses
            // into a single SUBACK response.
            // When the Server receives a SUBSCRIBE Packet from a Client, the Server MUST respond with a
            // SUBACK Packet. The SUBACK Packet MUST have the same Packet Identifier as the
            // SUBSCRIBE Packet that it is acknowledging.
            logger.debug("Message response: Send SUBACK back to client {}", this.clientId);
            this.registry.sendMessage(
                    ctx,
                    MqttMessageFactory.newMessage(
                            new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                            MqttPacketIdVariableHeader.from(packetId),
                            new MqttSubAckPayload(grantedQosLevels)),
                    this.clientId,
                    packetId,
                    true);
        });
    }

    protected void onUnsubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage msg) {
        if (!this.connected) {
            logger.debug("Protocol violation: Client {} must first sent a CONNECT message, now received UNSUBSCRIBE message, disconnect the client", this.clientId);
            ctx.close();
            return;
        }

        logger.debug("Message received: Received UNSUBSCRIBE message from client {} user {} topics {}", this.clientId, this.userName, ArrayUtils.toString(msg.payload().topics()));

        int packetId = msg.variableHeader().packetId();

        // Pass message to processor
        this.communicator.sendToProcessor(InternalMessage.fromMqttMessage(this.version, this.cleanSession, this.clientId, this.userName, this.config.getString("broker.id"), msg));

        // The Server MUST respond to an UNSUBSUBCRIBE request by sending an UNSUBACK packet. The
        // UNSUBACK Packet MUST have the same Packet Identifier as the UNSUBSCRIBE Packet.
        // Even where no Topic Subscriptions are deleted, the Server MUST respond with an
        // UNSUBACK.
        // If a Server receives an UNSUBSCRIBE packet that contains multiple Topic Filters it MUST handle that
        // packet as if it had received a sequence of multiple UNSUBSCRIBE packets, except that it sends just one
        // UNSUBACK response.
        logger.debug("Message response: Send UNSUBACK back to client {}", this.clientId);
        this.registry.sendMessage(
                ctx,
                MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        MqttPacketIdVariableHeader.from(packetId),
                        null),
                this.clientId,
                packetId,
                true);
    }

    protected void onPingReq(ChannelHandlerContext ctx) {
        if (!this.connected) {
            logger.debug("Protocol violation: Client {} must first sent a CONNECT message, now received PINGREQ message, disconnect the client", this.clientId);
            ctx.close();
            return;
        }

        logger.debug("Message received: Received PINGREQ message from client {} user {}", this.clientId, this.userName);

        logger.debug("Response: Send PINGRESP back to client {}", this.clientId);
        this.registry.sendMessage(
                ctx,
                MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        null,
                        null),
                this.clientId,
                null,
                true);
    }

    protected void onDisconnect(ChannelHandlerContext ctx) {
        if (!this.connected) {
            logger.debug("Protocol violation: Client {} must first sent a CONNECT message, now received DISCONNECT message, disconnect the client", this.clientId);
            ctx.close();
            return;
        }

        logger.debug("Message received: Received CONNECT message from client {} user {}", this.clientId, this.userName);

        // If the Will Flag is set to 1 this indicates that, if the Connect request is accepted, a Will Message MUST be
        // stored on the Server and associated with the Network Connection. The Will Message MUST be published
        // when the Network Connection is subsequently closed unless the Will Message has been deleted by the
        // Server on receipt of a DISCONNECT Packet.
        this.willMessage = null;

        // On receipt of DISCONNECT the Server:
        // MUST discard any Will Message associated with the current connection without publishing it.
        // SHOULD close the Network Connection if the Client has not already done so.
        this.connected = false;
        ctx.close();

        // Pass message to processor
        this.communicator.sendToProcessor(InternalMessage.fromMqttMessage(this.version, this.cleanSession, this.clientId, this.userName, this.config.getString("broker.id"), true));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (this.connected) {

            logger.debug("Connection closed: Connection lost from client {} user {}", this.clientId, this.userName);

            // Pass message to processor
            this.communicator.sendToProcessor(InternalMessage.fromMqttMessage(this.version, this.cleanSession, this.clientId, this.userName, this.config.getString("broker.id"), false));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (this.connected) {
            logger.debug("Exception caught: Exception caught from client {} user {}: {}", this.clientId, this.userName, ExceptionUtils.getMessage(cause));
        }
        ctx.close();
    }
}
