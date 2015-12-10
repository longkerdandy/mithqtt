package com.github.longkerdandy.mithril.mqtt.http.resources;

import com.github.longkerdandy.mithril.mqtt.api.auth.Authenticator;
import com.github.longkerdandy.mithril.mqtt.api.auth.AuthorizeResult;
import com.github.longkerdandy.mithril.mqtt.api.comm.HttpCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.api.internal.Publish;
import com.github.longkerdandy.mithril.mqtt.api.metrics.MetricsService;
import com.github.longkerdandy.mithril.mqtt.http.entity.ErrorCode;
import com.github.longkerdandy.mithril.mqtt.http.entity.ErrorEntity;
import com.github.longkerdandy.mithril.mqtt.http.entity.ResultEntity;
import com.github.longkerdandy.mithril.mqtt.http.exception.AuthorizeException;
import com.github.longkerdandy.mithril.mqtt.http.exception.ValidateException;
import com.github.longkerdandy.mithril.mqtt.http.util.Validator;
import com.github.longkerdandy.mithril.mqtt.storage.redis.sync.RedisSyncStorage;
import com.github.longkerdandy.mithril.mqtt.util.Topics;
import com.sun.security.auth.UserPrincipal;
import io.dropwizard.auth.Auth;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.security.PermitAll;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MQTT Publish related resource
 */
@Path("/clients/{clientId}/publish")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.TEXT_PLAIN)
public class MqttPublishResource extends AbstractResource {

    private static final Logger logger = LoggerFactory.getLogger(MqttPublishResource.class);

    public MqttPublishResource(String serverId, Validator validator, RedisSyncStorage redis, HttpCommunicator communicator, Authenticator authenticator, MetricsService metrics) {
        super(serverId, validator, redis, communicator, authenticator, metrics);
    }

    @PermitAll
    @POST
    public ResultEntity<Boolean> publish(@PathParam("clientId") String clientId, @Auth UserPrincipal user, @QueryParam("protocol") @DefaultValue("4") byte protocol,
                                         @QueryParam("dup") @DefaultValue("false") boolean dup, @QueryParam("qos") @DefaultValue("0") int qos,
                                         @QueryParam("topicName") String topicName, @QueryParam("packetId") @DefaultValue("0") int packetId,
                                         String body) throws UnsupportedEncodingException {
        String userName = user.getName();
        MqttVersion version = MqttVersion.fromProtocolLevel(protocol);
        byte[] payload = body == null ? null : body.getBytes("ISO-8859-1");

        // HTTP interface require valid Client Id
        if (!this.validator.isClientIdValid(clientId)) {
            logger.debug("Protocol violation: Client id {} not valid based on configuration", clientId);
            throw new ValidateException(new ErrorEntity(ErrorCode.INVALID));
        }

        // The Topic Name in the PUBLISH Packet MUST NOT contain wildcard characters
        // Validate Topic Name based on configuration
        if (!this.validator.isTopicNameValid(topicName)) {
            logger.debug("Protocol violation: Client {} sent PUBLISH message contains invalid topic name {}", clientId, topicName);
            throw new ValidateException(new ErrorEntity(ErrorCode.INVALID));
        }

        List<String> topicLevels = Topics.sanitizeTopicName(topicName);

        logger.debug("Message received: Received PUBLISH message from client {} user {} topic {}", clientId, user.getName(), topicName);

        AuthorizeResult result = this.authenticator.authPublish(clientId, userName, topicName, qos, false);
        // Authorize successful
        if (result == AuthorizeResult.OK) {
            logger.trace("Authorization succeed: Publish to topic {} authorized for client {}", topicName, clientId);

            // Construct Internal Message
            Publish publish = new Publish(topicName, packetId, payload);
            InternalMessage<Publish> msg = new InternalMessage<>(MqttMessageType.PUBLISH, dup, MqttQoS.valueOf(qos), false, version, clientId, userName, this.serverId, publish);

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
            subscriptions.forEach((cid, q) -> {

                // Compare publish QoS and subscription QoS
                MqttQoS fQos = qos > q.value() ? q : MqttQoS.valueOf(qos);

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

                // Construct Internal Message
                Publish p = new Publish(topicName, pid, payload);
                InternalMessage<Publish> m = new InternalMessage<>(MqttMessageType.PUBLISH, false, fQos, false, MqttVersion.MQTT_3_1_1, cid, null, null, p);

                // Forward to recipient
                boolean d = false;
                String bid = this.redis.getConnectedNode(cid);
                if (StringUtils.isNotBlank(bid)) {
                    logger.trace("Communicator sending: Send PUBLISH message to broker {} for client {} subscription", bid, cid);
                    d = true;
                    this.communicator.sendToBroker(bid, m);
                }

                // In the QoS 1 delivery protocol, the Sender
                // MUST treat the PUBLISH Packet as “unacknowledged” until it has received the corresponding
                // PUBACK packet from the receiver.
                // In the QoS 2 delivery protocol, the Sender
                // MUST treat the PUBLISH packet as “unacknowledged” until it has received the corresponding
                // PUBREC packet from the receiver.
                if (fQos == MqttQoS.AT_LEAST_ONCE || fQos == MqttQoS.EXACTLY_ONCE) {
                    logger.trace("Add in-flight: Add in-flight PUBLISH message {} with QoS {} for client {}", pid, fQos, cid);
                    this.redis.addInFlightMessage(cid, pid, m, d);
                }
            });

            // Pass message to 3rd party application
            this.communicator.sendToApplication(msg);

            return new ResultEntity<>(true);
        } else {
            throw new AuthorizeException(new ErrorEntity(ErrorCode.UNAUTHORIZED));
        }
    }
}
