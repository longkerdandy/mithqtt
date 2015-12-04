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
import com.github.longkerdandy.mithril.mqtt.storage.redis.sync.RedisSyncStorage;
import com.github.longkerdandy.mithril.mqtt.util.Topics;
import io.dropwizard.auth.Auth;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public MqttPublishResource(String serverId, RedisSyncStorage redis, HttpCommunicator communicator, Authenticator authenticator, MetricsService metrics) {
        super(serverId, redis, communicator, authenticator, metrics);
    }

    public ResultEntity<Boolean> publish(@PathParam("clientId") String clientId, @Auth String userName, @QueryParam("protocol") @DefaultValue("4") byte protocol,
                                         @QueryParam("dup") @DefaultValue("false") boolean dup, @QueryParam("qos") int qos, @QueryParam("retain") @DefaultValue("false") boolean retain,
                                         @QueryParam("topicName") String topicName, @QueryParam("packetId") @DefaultValue("0") int packetId,
                                         String body) throws UnsupportedEncodingException {
        MqttVersion version = MqttVersion.fromProtocolLevel(protocol);
        List<String> topicLevels = Topics.sanitizeTopicName(topicName);
        byte[] payload = body == null ? null : body.getBytes("ISO-8859-1");

        AuthorizeResult result = this.authenticator.authPublish(clientId, userName, topicName, qos, retain);
        // Authorize successful
        if (result == AuthorizeResult.OK) {
            logger.trace("Authorization succeed: Publish to topic {} authorized for client {}", topicName, clientId);

            // Construct Internal Message
            Publish publish = new Publish(topicName, packetId, payload);
            InternalMessage<Publish> msg = new InternalMessage<>(MqttMessageType.PUBLISH, dup, MqttQoS.valueOf(qos), retain, version, clientId, userName, this.serverId, publish);

            // If the RETAIN flag is set to 1, in a PUBLISH Packet sent by a Client to a Server, the Server MUST store
            // the Application Message and its QoS, so that it can be delivered to future subscribers whose
            // subscriptions match its topic name. When a new subscription is established, the last
            // retained message, if any, on each matching topic name MUST be sent to the subscriber.
            if (retain) {
                // If the Server receives a QoS 0 message with the RETAIN flag set to 1 it MUST discard any message
                // previously retained for that topic. It SHOULD store the new QoS 0 message as the new retained
                // message for that topic, but MAY choose to discard it at any time - if this happens there will be no retained
                // message for that topic.
                if (qos == MqttQoS.AT_MOST_ONCE.value() || payload == null || payload.length == 0) {
                    logger.trace("Clear retain: Clear retain messages for topic {} by client {}", topicName, clientId);
                    this.redis.removeAllRetainMessage(topicLevels);
                }

                // A PUBLISH Packet with a RETAIN flag set to 1 and a payload containing zero bytes will be processed as
                // normal by the Server and sent to Clients with a subscription matching the topic name. Additionally any
                // existing retained message with the same topic name MUST be removed and any future subscribers for
                // the topic will not receive a retained message. “As normal” means that the RETAIN flag is
                // not set in the message received by existing Clients. A zero byte retained message MUST NOT be stored
                // as a retained message on the Server
                if (payload != null && payload.length > 0) {
                    logger.trace("Add retain: Add retain messages for topic {} by client {}", topicName, clientId);
                    this.redis.addRetainMessage(topicLevels, msg);
                }
            }

            // In the QoS 0 delivery protocol, the Receiver
            // Accepts ownership of the message when it receives the PUBLISH packet.
            // In the QoS 1 delivery protocol, the Receiver
            // After it has sent a PUBACK Packet the Receiver MUST treat any incoming PUBLISH packet that
            // contains the same Packet Identifier as being a new publication, irrespective of the setting of its
            // DUP flag.
            if (qos == MqttQoS.AT_MOST_ONCE.value() || qos == MqttQoS.AT_LEAST_ONCE.value()
                    // In the QoS 2 delivery protocol, the Receiver
                    // Until it has received the corresponding PUBREL packet, the Receiver MUST acknowledge any
                    // subsequent PUBLISH packet with the same Packet Identifier by sending a PUBREC. It MUST
                    // NOT cause duplicate messages to be delivered to any onward recipients in this case.
                    // The recipient of a Control Packet that contains the DUP flag set to 1 cannot assume that it has
                    // seen an earlier copy of this packet.
                    || (qos == MqttQoS.EXACTLY_ONCE.value() && this.redis.addQoS2MessageId(clientId, packetId))) {

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
            }

            return new ResultEntity<>(true);
        } else {
            throw new AuthorizeException(new ErrorEntity(ErrorCode.UNAUTHORIZED));
        }
    }
}
