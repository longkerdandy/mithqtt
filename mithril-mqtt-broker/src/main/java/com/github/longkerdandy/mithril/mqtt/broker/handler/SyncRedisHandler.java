package com.github.longkerdandy.mithril.mqtt.broker.handler;

import com.github.longkerdandy.mithril.mqtt.api.auth.Authenticator;
import com.github.longkerdandy.mithril.mqtt.api.auth.AuthorizeResult;
import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.internal.Disconnect;
import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.api.internal.PacketId;
import com.github.longkerdandy.mithril.mqtt.api.internal.Publish;
import com.github.longkerdandy.mithril.mqtt.broker.session.SessionRegistry;
import com.github.longkerdandy.mithril.mqtt.broker.util.Validator;
import com.github.longkerdandy.mithril.mqtt.storage.redis.sync.RedisSyncStorage;
import com.github.longkerdandy.mithril.mqtt.util.Topics;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.longkerdandy.mithril.mqtt.util.UUIDs.shortUuid;

/**
 * Synchronous MQTT Handler using Redis
 */
public class SyncRedisHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private static final Logger logger = LoggerFactory.getLogger(SyncRedisHandler.class);

    protected final Authenticator authenticator;
    protected final BrokerCommunicator communicator;
    protected final RedisSyncStorage redis;
    protected final SessionRegistry registry;
    protected final PropertiesConfiguration config;
    protected final Validator validator;

    // session state
    protected MqttVersion version;
    protected String clientId;
    protected String userName;
    protected String brokerId;
    protected boolean connected;
    protected boolean cleanSession;
    protected int keepAlive;
    protected MqttPublishMessage willMessage;

    public SyncRedisHandler(Authenticator authenticator, BrokerCommunicator communicator, RedisSyncStorage redis, SessionRegistry registry, PropertiesConfiguration config, Validator validator) {
        this.authenticator = authenticator;
        this.communicator = communicator;
        this.redis = redis;
        this.registry = registry;
        this.config = config;
        this.validator = validator;

        this.brokerId = config.getString("broker.id");
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
        // Validate User Name based on configuration
        // Validate Password based on configuration
        if (userNameFlag) {
            if (StringUtils.isBlank(this.userName) || !this.validator.isUserNameValid(this.userName))
                malformed = true;
        } else {
            if (StringUtils.isNotBlank(this.userName) || passwordFlag) malformed = true;
        }
        if (passwordFlag) {
            if (StringUtils.isBlank(password) || !this.validator.isPasswordValid(password)) malformed = true;
        } else {
            if (StringUtils.isNotBlank(password)) malformed = true;
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

        AuthorizeResult result = this.authenticator.authConnect(this.clientId, this.userName, password);
        // Authorize successful
        if (result == AuthorizeResult.OK) {
            logger.trace("Authorization succeed: Connection authorized for client {} user {}", this.clientId, this.userName);

            // Mark client's connected broker node
            logger.trace("Update node: Mark client {} connected to broker {}", this.clientId, this.brokerId);
            String previous = this.redis.updateConnectedNode(this.clientId, this.brokerId);

            // If the Server accepts a connection with CleanSession set to 1, the Server MUST set Session Present to 0
            // in the CONNACK packet in addition to setting a zero return code in the CONNACK packet
            // If the Server accepts a connection with CleanSession set to 0, the value set in Session Present depends
            // on whether the Server already has stored Session state for the supplied client ID. If the Server has stored
            // Session state, it MUST set Session Present to 1 in the CONNACK packet. If the Server
            // does not have stored Session state, it MUST set Session Present to 0 in the CONNACK packet. This is in
            // addition to setting a zero return code in the CONNACK packet.
            int exist = this.redis.getSessionExist(this.clientId);
            boolean sessionPresent = (exist >= 0) && !this.cleanSession;

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
                if (exist == 0) {
                    logger.trace("Message response: Resend In-Flight messages to client {}", this.clientId);
                    for (InternalMessage inFlight : this.redis.getAllInFlightMessages(this.clientId)) {
                        if (inFlight.getMessageType() == MqttMessageType.PUBLISH) {
                            this.registry.sendMessage(ctx, inFlight.toMqttMessage(), this.clientId, ((Publish) inFlight.getPayload()).getPacketId(), false);
                        } else if (inFlight.getMessageType() == MqttMessageType.PUBREL) {
                            this.registry.sendMessage(ctx, inFlight.toMqttMessage(), this.clientId, ((PacketId) inFlight.getPayload()).getPacketId(), false);
                        }
                    }
                    ctx.flush();
                } else if (exist == 1) {
                    logger.trace("Clear session: Clear session state for client {} because former connection is clean session", this.clientId);
                    this.redis.removeAllSessionState(this.clientId);
                }
            }
            // If CleanSession is set to 1, the Client and Server MUST discard any previous Session and start a new
            // one. This Session lasts as long as the Network Connection. State data associated with this Session
            // MUST NOT be reused in any subsequent Session.
            // When CleanSession is set to 1 the Client and Server need not process the deletion of state atomically.
            else {
                if (exist >= 0) {
                    logger.trace("Clear session: Clear session state for client {} because current connection is clean session", this.clientId);
                    this.redis.removeAllSessionState(this.clientId);
                }
            }

            // If the ClientId represents a Client already connected to the Server then the Server MUST
            // disconnect the existing Client
            ChannelHandlerContext lastSession = this.registry.removeSession(this.clientId);
            if (lastSession != null) {
                logger.trace("Disconnect: Try to disconnect existed client {}", this.clientId);
                lastSession.close();
            }
            if (StringUtils.isNotBlank(previous) && !previous.equals(this.brokerId)) {
                logger.trace("Communicator sending: Send DISCONNECT message to broker {} to drop the existing client {}", previous, this.clientId);
                InternalMessage<Disconnect> disconnect = new InternalMessage<>(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false,
                        MqttVersion.MQTT_3_1_1, this.clientId, null, null, new Disconnect(false, false));
                this.communicator.sendToBroker(previous, disconnect);
            }

            // If the Will Flag is set to 1 this indicates that, if the Connect request is accepted, a Will Message MUST be
            // stored on the Server and associated with the Network Connection. The Will Message MUST be published
            // when the Network Connection is subsequently closed unless the Will Message has been deleted by the
            // Server on receipt of a DISCONNECT Packet.
            String willTopic = msg.payload().willTopic();
            String willMessage = msg.payload().willMessage();
            if (msg.variableHeader().willFlag()
                    && StringUtils.isNoneEmpty(willTopic) && this.validator.isTopicNameValid(willTopic)
                    && StringUtils.isNotEmpty(willMessage)) {
                this.willMessage = (MqttPublishMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.PUBLISH, false, msg.variableHeader().willQos(), msg.variableHeader().willRetain(), 0),
                        MqttPublishVariableHeader.from(willTopic),
                        Unpooled.wrappedBuffer(willMessage.getBytes())
                );
            }

            // If the Keep Alive value is non-zero and the Server does not receive a Control Packet from the Client
            // within one and a half times the Keep Alive time period, it MUST disconnect the Network Connection to the
            // Client as if the network had failed
            this.keepAlive = msg.variableHeader().keepAlive();
            if (this.keepAlive <= 0 || this.keepAlive > this.config.getInt("mqtt.keepalive.max"))
                this.keepAlive = this.config.getInt("mqtt.keepalive.default");
            if (ctx.pipeline().names().contains("idleHandler"))
                ctx.pipeline().remove("idleHandler");
            ctx.pipeline().addFirst("idleHandler", new IdleStateHandler(0, 0, Math.round(this.keepAlive * 1.5f)));

            // Save connection state, add to local registry
            this.connected = true;
            this.registry.saveSession(this.clientId, ctx);

            // Mark client's session as existed
            this.redis.updateSessionExist(this.clientId, this.cleanSession);

            // Pass message to 3rd party application
            this.communicator.sendToApplication(InternalMessage.fromMqttMessage(this.version, this.clientId, this.userName, this.brokerId, msg));
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
    }

    protected void onPublish(ChannelHandlerContext ctx, MqttPublishMessage msg) {
        if (!this.connected) {
            logger.debug("Protocol violation: Client {} must first sent a CONNECT message, now received PUBLISH message, disconnect the client", this.clientId);
            ctx.close();
            return;
        }

        // boolean dup = msg.fixedHeader().dup();
        MqttQoS qos = msg.fixedHeader().qos();
        boolean retain = msg.fixedHeader().retain();
        String topicName = msg.variableHeader().topicName();
        List<String> topicLevels = Topics.sanitizeTopicName(topicName);
        int packetId = msg.variableHeader().packetId();

        // The Topic Name in the PUBLISH Packet MUST NOT contain wildcard characters
        // Validate Topic Name based on configuration
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

        AuthorizeResult result = this.authenticator.authPublish(this.clientId, this.userName, topicName, qos.value(), retain);
        // Authorize successful
        if (result == AuthorizeResult.OK) {
            logger.trace("Authorization succeed: Publish to topic {} authorized for client {}", topicName, this.clientId);

            // If the RETAIN flag is set to 1, in a PUBLISH Packet sent by a Client to a Server, the Server MUST store
            // the Application Message and its QoS, so that it can be delivered to future subscribers whose
            // subscriptions match its topic name. When a new subscription is established, the last
            // retained message, if any, on each matching topic name MUST be sent to the subscriber.
            if (retain) {
                // If the Server receives a QoS 0 message with the RETAIN flag set to 1 it MUST discard any message
                // previously retained for that topic. It SHOULD store the new QoS 0 message as the new retained
                // message for that topic, but MAY choose to discard it at any time - if this happens there will be no retained
                // message for that topic.
                if (qos == MqttQoS.AT_MOST_ONCE || msg.payload() == null || msg.payload().readableBytes() == 0) {
                    logger.trace("Clear retain: Clear retain messages for topic {} by client {}", topicName, this.clientId);
                    this.redis.removeAllRetainMessage(topicLevels);
                }

                // A PUBLISH Packet with a RETAIN flag set to 1 and a payload containing zero bytes will be processed as
                // normal by the Server and sent to Clients with a subscription matching the topic name. Additionally any
                // existing retained message with the same topic name MUST be removed and any future subscribers for
                // the topic will not receive a retained message. “As normal” means that the RETAIN flag is
                // not set in the message received by existing Clients. A zero byte retained message MUST NOT be stored
                // as a retained message on the Server
                if (msg.payload() != null && msg.payload().readableBytes() > 0) {
                    logger.trace("Add retain: Add retain messages for topic {} by client {}", topicName, this.clientId);
                    this.redis.addRetainMessage(topicLevels, InternalMessage.fromMqttMessage(this.version, this.clientId, this.userName, this.brokerId, msg));
                }
            }

            // In the QoS 0 delivery protocol, the Receiver
            // Accepts ownership of the message when it receives the PUBLISH packet.
            if (qos == MqttQoS.AT_MOST_ONCE) {
                onwardRecipients(msg);
            }
            // In the QoS 1 delivery protocol, the Receiver
            // After it has sent a PUBACK Packet the Receiver MUST treat any incoming PUBLISH packet that
            // contains the same Packet Identifier as being a new publication, irrespective of the setting of its
            // DUP flag.
            else if (qos == MqttQoS.AT_LEAST_ONCE) {
                onwardRecipients(msg);
            }
            // In the QoS 2 delivery protocol, the Receiver
            // Until it has received the corresponding PUBREL packet, the Receiver MUST acknowledge any
            // subsequent PUBLISH packet with the same Packet Identifier by sending a PUBREC. It MUST
            // NOT cause duplicate messages to be delivered to any onward recipients in this case.
            else if (qos == MqttQoS.EXACTLY_ONCE) {
                // The recipient of a Control Packet that contains the DUP flag set to 1 cannot assume that it has
                // seen an earlier copy of this packet.
                if (this.redis.addQoS2MessageId(this.clientId, packetId)) {
                    onwardRecipients(msg);
                }
            }

            // Pass message to 3rd party application
            this.communicator.sendToApplication(InternalMessage.fromMqttMessage(this.version, this.clientId, this.userName, this.brokerId, msg));

        } else {
            logger.trace("Authorization failed: Publish to topic {} unauthorized for client {}", topicName, this.clientId);
        }
    }

    /**
     * Forward MQTT PUBLISH message to its recipients
     *
     * @param msg MQTT PUBLISH Message
     */
    protected void onwardRecipients(MqttPublishMessage msg) {
        List<String> topicLevels = Topics.sanitizeTopicName(msg.variableHeader().topicName());

        // forge bytes payload
        ByteBuf buf = msg.payload().duplicate();
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);

        // When sending a PUBLISH Packet to a Client the Server MUST set the RETAIN flag to 1 if a message is
        // sent as a result of a new subscription being made by a Client. It MUST set the RETAIN
        // flag to 0 when a PUBLISH Packet is sent to a Client because it matches an established subscription
        // regardless of how the flag was set in the message it received.

        // The Server uses a PUBLISH Packet to send an Application Message to each Client which has a
        // matching subscription.
        // When Clients make subscriptions with Topic Filters that include wildcards, it is possible for a Client’s
        // subscriptions to overlap so that a published message might match multiple filters. In this case the Server
        // MUST deliver the message to the Client respecting the maximum QoS of all the matching subscriptions.
        // In addition, the Server MAY deliver further copies of the message, one for each
        // additional matching subscription and respecting the subscription’s QoS in each case.
        Map<String, MqttQoS> subscriptions = new HashMap<>();
        this.redis.getMatchSubscriptions(topicLevels, subscriptions);
        subscriptions.forEach((cid, qos) -> {

            // Compare publish QoS and subscription QoS
            MqttQoS fQos = msg.fixedHeader().qos().value() > qos.value() ? qos : msg.fixedHeader().qos();

            // Each time a Client sends a new packet of one of these
            // types it MUST assign it a currently unused Packet Identifier. If a Client re-sends a
            // particular Control Packet, then it MUST use the same Packet Identifier in subsequent re-sends of that
            // packet. The Packet Identifier becomes available for reuse after the Client has processed the
            // corresponding acknowledgement packet. In the case of a QoS 1 PUBLISH this is the corresponding
            // PUBACK; in the case of QoS 2 it is PUBCOMP. For SUBSCRIBE or UNSUBSCRIBE it is the
            // corresponding SUBACK or UNSUBACK. The same conditions apply to a Server when it
            // sends a PUBLISH with QoS > 0
            // A PUBLISH Packet MUST NOT contain a Packet Identifier if its QoS value is set to
            int pid = 0;
            if (fQos == MqttQoS.AT_LEAST_ONCE || fQos == MqttQoS.EXACTLY_ONCE) {
                pid = this.redis.getNextPacketId(cid);
            }

            Publish p = new Publish(msg.variableHeader().topicName(), pid, bytes);
            InternalMessage<Publish> publish = new InternalMessage<>(MqttMessageType.PUBLISH, false, fQos, false,
                    MqttVersion.MQTT_3_1_1, cid, null, null, p);

            // Forward to recipient
            boolean dup = false;
            String bid = this.redis.getConnectedNode(cid);
            if (StringUtils.isNotBlank(bid)) {
                if (bid.equals(this.brokerId)) {
                    logger.trace("Message forward: Send PUBLISH message to client {}", cid);
                    dup = true;
                    this.registry.sendMessage(publish.toMqttMessage(), cid, pid, true);
                } else {
                    logger.trace("Communicator sending: Send PUBLISH message to broker {} for client {} subscription", bid, cid);
                    dup = true;
                    this.communicator.sendToBroker(bid, publish);
                }
            }

            // In the QoS 1 delivery protocol, the Sender
            // MUST treat the PUBLISH Packet as “unacknowledged” until it has received the corresponding
            // PUBACK packet from the receiver.
            // In the QoS 2 delivery protocol, the Sender
            // MUST treat the PUBLISH packet as “unacknowledged” until it has received the corresponding
            // PUBREC packet from the receiver.
            if (fQos == MqttQoS.AT_LEAST_ONCE || fQos == MqttQoS.EXACTLY_ONCE) {
                logger.trace("Add in-flight: Add in-flight PUBLISH message {} with QoS {} for client {}", pid, fQos, cid);
                this.redis.addInFlightMessage(cid, pid, publish, dup);
            }
        });
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
        this.redis.removeInFlightMessage(this.clientId, packetId);

        // Send back PUBREL
        MqttMessage pubrel = MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                MqttPacketIdVariableHeader.from(packetId),
                null);
        logger.trace("Message response: Send PUBREL back to client {}", this.clientId);
        this.registry.sendMessage(ctx, pubrel, this.clientId, packetId, true);

        // Save PUBREL as in-flight message
        InternalMessage internal = InternalMessage.fromMqttMessage(this.version, this.clientId, this.userName, this.brokerId, pubrel);
        logger.trace("Add in-flight: Add In-Flight PUBREL message {} for client {}", packetId, this.clientId);
        this.redis.addInFlightMessage(this.clientId, packetId, internal, true);
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

        // Validate Topic Filter based on configuration
        for (MqttTopicSubscription subscription : requestSubscriptions) {
            if (!this.validator.isTopicFilterValid(subscription.topic())) {
                logger.debug("Protocol violation: Client {} subscription {} is not valid based on configuration, disconnect the client", this.clientId, subscription.topic());
                ctx.close();
                return;
            }
        }

        logger.debug("Message received: Received SUBSCRIBE message from client {} user {}", this.clientId, this.userName);

        // Authorize client subscribe using provided Authenticator
        List<MqttGrantedQoS> grantedQosLevels = this.authenticator.authSubscribe(this.clientId, this.userName, requestSubscriptions);
        logger.trace("Authorization granted: Subscribe to topic {} granted as {} for client {}", ArrayUtils.toString(msg.payload().subscriptions()), ArrayUtils.toString(grantedQosLevels), this.clientId);

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

        for (int i = 0; i < requestSubscriptions.size(); i++) {

            MqttGrantedQoS grantedQoS = grantedQosLevels.get(i);
            String topic = requestSubscriptions.get(i).topic();
            List<String> topicLevels = Topics.sanitize(topic);

            // Granted only
            if (grantedQoS != MqttGrantedQoS.FAILURE) {

                // If a Server receives a SUBSCRIBE Packet containing a Topic Filter that is identical to an existing
                // Subscription’s Topic Filter then it MUST completely replace that existing Subscription with a new
                // Subscription. The Topic Filter in the new Subscription will be identical to that in the previous Subscription,
                // although its maximum QoS value could be different. Any existing retained messages matching the Topic
                // Filter MUST be re-sent, but the flow of publications MUST NOT be interrupted.
                // Where the Topic Filter is not identical to any existing Subscription’s filter, a new Subscription is created
                // and all matching retained messages are sent.
                logger.trace("Update subscription: Update client {} subscription with topic {} QoS {}", this.clientId, topic, grantedQoS);
                this.redis.updateSubscription(this.clientId, topicLevels, MqttQoS.valueOf(grantedQoS.value()));

                // The Server is permitted to start sending PUBLISH packets matching the Subscription before the Server
                // sends the SUBACK Packet.
                for (InternalMessage<Publish> retain : this.redis.getMatchRetainMessages(topicLevels)) {

                    // Set recipient client id
                    retain.setClientId(this.clientId);

                    // Compare publish QoS and subscription QoS
                    if (retain.getQos().value() > grantedQoS.value()) {
                        retain.setQos(MqttQoS.valueOf(grantedQoS.value()));
                    }

                    // Set packet id
                    int pid = 0;
                    if (retain.getQos() == MqttQoS.AT_LEAST_ONCE || retain.getQos() == MqttQoS.EXACTLY_ONCE) {
                        pid = this.redis.getNextPacketId(this.clientId);
                        retain.getPayload().setPacketId(pid);
                    }

                    // Forward to recipient
                    logger.trace("Message forward: Send retained PUBLISH message to client {} subscription with topic {}", this.clientId, topic);
                    this.registry.sendMessage(ctx, retain.toMqttMessage(), this.clientId, pid, true);

                    // In the QoS 1 delivery protocol, the Sender
                    // MUST treat the PUBLISH Packet as “unacknowledged” until it has received the corresponding
                    // PUBACK packet from the receiver.
                    // In the QoS 2 delivery protocol, the Sender
                    // MUST treat the PUBLISH packet as “unacknowledged” until it has received the corresponding
                    // PUBREC packet from the receiver.
                    if (retain.getQos() == MqttQoS.AT_LEAST_ONCE || retain.getQos() == MqttQoS.EXACTLY_ONCE) {
                        logger.trace("Add in-flight: Add in-flight PUBLISH message {} with QoS {} for client {}", pid, retain.getQos(), this.clientId);
                        this.redis.addInFlightMessage(this.clientId, pid, retain, true);
                    }
                }
            }
        }

        // Pass message to 3rd party application
        this.communicator.sendToApplication(InternalMessage.fromMqttMessage(this.version, this.clientId, this.userName, this.brokerId, msg));
    }

    protected void onUnsubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage msg) {
        if (!this.connected) {
            logger.debug("Protocol violation: Client {} must first sent a CONNECT message, now received UNSUBSCRIBE message, disconnect the client", this.clientId);
            ctx.close();
            return;
        }

        // Validate Topic Filter based on configuration
        for (String topic : msg.payload().topics()) {
            if (!this.validator.isTopicFilterValid(topic)) {
                logger.debug("Protocol violation: Client {} un-subscription {} is not valid based on configuration, disconnect the client", this.clientId, topic);
                ctx.close();
                return;
            }
        }

        logger.debug("Message received: Received UNSUBSCRIBE message from client {} user {} topics {}", this.clientId, this.userName, ArrayUtils.toString(msg.payload().topics()));

        int packetId = msg.variableHeader().packetId();

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

        // The Topic Filters (whether they contain wildcards or not) supplied in an UNSUBSCRIBE packet MUST be
        // compared character-by-character with the current set of Topic Filters held by the Server for the Client. If
        // any filter matches exactly then its owning Subscription is deleted, otherwise no additional processing
        // occurs
        // If a Server deletes a Subscription:
        // It MUST stop adding any new messages for delivery to the Client.
        //1 It MUST complete the delivery of any QoS 1 or QoS 2 messages which it has started to send to
        // the Client.
        // It MAY continue to deliver any existing messages buffered for delivery to the Client.
        msg.payload().topics().forEach(topic -> {
            logger.trace("Remove subscription: Remove client {} subscription with topic {}", this.clientId, topic);
            this.redis.removeSubscription(this.clientId, Topics.sanitize(topic));
        });

        // Pass message to 3rd party application
        this.communicator.sendToApplication(InternalMessage.fromMqttMessage(this.version, this.clientId, this.userName, this.brokerId, msg));
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

        logger.debug("Message received: Received DISCONNECT message from client {} user {}", this.clientId, this.userName);

        // If the Will Flag is set to 1 this indicates that, if the Connect request is accepted, a Will Message MUST be
        // stored on the Server and associated with the Network Connection. The Will Message MUST be published
        // when the Network Connection is subsequently closed unless the Will Message has been deleted by the
        // Server on receipt of a DISCONNECT Packet.
        this.willMessage = null;

        // On receipt of DISCONNECT the Server:
        // MUST discard any Will Message associated with the current connection without publishing it.
        // SHOULD close the Network Connection if the Client has not already done so.
        this.connected = false;

        // Test if client already reconnected to this broker
        if (this.registry.removeSession(this.clientId, ctx)) {

            // Test if client already reconnected to another broker
            if (this.redis.removeConnectedNode(this.clientId, this.brokerId)) {

                // Remove connected node
                logger.trace("Remove node: Mark client {} disconnected from broker {}", this.clientId, this.brokerId);

                // If CleanSession is set to 1, the Client and Server MUST discard any previous Session and start a new
                // one. This Session lasts as long as the Network Connection. State data associated with this Session
                // MUST NOT be reused in any subsequent Session.
                // When CleanSession is set to 1 the Client and Server need not process the deletion of state atomically.
                if (this.cleanSession) {
                    logger.trace("Clear session: Clear session state for client {} because current connection is clean session", this.clientId);
                    this.redis.removeAllSessionState(this.clientId);
                }

                // Pass message to 3rd party application
                this.communicator.sendToApplication(InternalMessage.fromMqttMessage(this.version, this.clientId, this.userName, this.brokerId, this.cleanSession, true));
            }
        }

        // Make sure connection is closed
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (this.connected) {

            logger.debug("Connection closed: Connection lost from client {} user {}", this.clientId, this.userName);

            // Test if client already reconnected to this broker
            if (this.registry.removeSession(this.clientId, ctx)) {

                // Test if client already reconnected to another broker
                if (this.redis.removeConnectedNode(this.clientId, this.brokerId)) {

                    // Remove connected node
                    logger.trace("Remove node: Mark client {} disconnected from broker {}", this.clientId, this.brokerId);

                    // If CleanSession is set to 1, the Client and Server MUST discard any previous Session and start a new
                    // one. This Session lasts as long as the Network Connection. State data associated with this Session
                    // MUST NOT be reused in any subsequent Session.
                    // When CleanSession is set to 1 the Client and Server need not process the deletion of state atomically.
                    if (this.cleanSession) {
                        logger.trace("Clear session: Clear session state for client {} because current connection is clean session", this.clientId);
                        this.redis.removeAllSessionState(this.clientId);
                    }

                    // Pass message to 3rd party application
                    this.communicator.sendToApplication(InternalMessage.fromMqttMessage(this.version, this.clientId, this.userName, this.brokerId, this.cleanSession, false));
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
            if (this.willMessage != null) {

                MqttQoS willQos = this.willMessage.fixedHeader().qos();
                String willTopic = this.willMessage.variableHeader().topicName();
                boolean willRetain = this.willMessage.fixedHeader().retain();

                AuthorizeResult result = this.authenticator.authPublish(this.clientId, this.userName, willTopic, willQos.value(), willRetain);
                // Authorize successful
                if (result == AuthorizeResult.OK) {
                    logger.trace("Authorization succeed: WILL message to topic {} authorized for client {}", willTopic, this.clientId);

                    // Onward to recipients
                    onwardRecipients(this.willMessage);
                }
                // Authorize failed
                else {
                    logger.trace("Authorization failed: WILL message to topic {} unauthorized for client {}", willTopic, this.clientId);
                }
            }
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.ALL_IDLE) {
                logger.debug("Protocol violation: Client {} has been idle beyond keep alive time, disconnect the client", this.clientId);
                ctx.close();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (this.connected) {
            logger.debug("Exception caught: Exception caught from client {} user {}: ", this.clientId, this.userName, cause);
        }
        ctx.close();
    }
}
