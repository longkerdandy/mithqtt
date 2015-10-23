package com.github.longkerdandy.mithril.mqtt.processor.comm;

import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorListener;
import com.github.longkerdandy.mithril.mqtt.api.internal.*;
import com.github.longkerdandy.mithril.mqtt.storage.redis.sync.RedisSyncStorage;
import com.github.longkerdandy.mithril.mqtt.util.Topics;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckReturnCode;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Processor Listener Implementation
 */
public class ProcessorListenerImpl implements ProcessorListener {

    private static final Logger logger = LoggerFactory.getLogger(ProcessorListener.class);

    private final ProcessorCommunicator communicator;
    private final RedisSyncStorage redis;

    public ProcessorListenerImpl(ProcessorCommunicator communicator, RedisSyncStorage redis) {
        this.communicator = communicator;
        this.redis = redis;
    }

    @Override
    public void onConnect(InternalMessage<Connect> msg) {
        // first, mark client's connected broker node
        logger.trace("Update node: Mark client {} connected to broker {}", msg.getClientId(), msg.getBrokerId());
        String previous = this.redis.updateConnectedNode(msg.getClientId(), msg.getBrokerId());

        // If the ClientId represents a Client already connected to the Server then the Server MUST
        // disconnect the existing Client
        if (StringUtils.isNotBlank(previous) && !previous.equals(msg.getBrokerId())) {
            logger.trace("Communicator sending: Send DISCONNECT message to broker {} to drop the existing client {}", msg.getBrokerId(), msg.getClientId());
            InternalMessage<Disconnect> m = new InternalMessage<>(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false,
                    MqttVersion.MQTT_3_1_1, false, msg.getClientId(), null, null, new Disconnect(false));
            this.communicator.sendToBroker(previous, m);
        }

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
        int sessionExist = this.redis.getSessionExist(msg.getClientId());
        if (!msg.isCleanSession()) {
            if (sessionExist == 0) {
                logger.trace("Communicator sending: Resend In-Flight messages to broker {} for client {}", msg.getBrokerId(), msg.getClientId());
                for (InternalMessage inFlight : this.redis.getAllInFlightMessages(msg.getClientId())) {
                    this.communicator.sendToBroker(msg.getBrokerId(), inFlight);
                }
            } else if (sessionExist == 1) {
                logger.trace("Clear session: Clear session state for client {} because former connection is clean session", msg.getClientId());
                this.redis.removeAllSessionState(msg.getClientId());
            }
        }
        // If CleanSession is set to 1, the Client and Server MUST discard any previous Session and start a new
        // one. This Session lasts as long as the Network Connection. State data associated with this Session
        // MUST NOT be reused in any subsequent Session.
        // When CleanSession is set to 1 the Client and Server need not process the deletion of state atomically.
        else {
            if (sessionExist >= 0) {
                logger.trace("Clear session: Clear session state for client {} because current connection is clean session", msg.getClientId());
                this.redis.removeAllSessionState(msg.getClientId());
            }
        }

        // If the Will Flag is set to 1 this indicates that, if the Connect request is accepted, a Will Message MUST be
        // stored on the Server and associated with the Network Connection. The Will Message MUST be published
        // when the Network Connection is subsequently closed unless the Will Message has been deleted by the
        // Server on receipt of a DISCONNECT Packet.
        // TODO: Deal with Will Message after Netty fixed the field type

        // last, mark client's session as existed
        this.redis.updateSessionExist(msg.getClientId(), msg.isCleanSession());
    }

    @Override
    public void onPublish(InternalMessage<Publish> msg) {
        List<String> topicLevels = Topics.sanitizeTopicName(msg.getPayload().getTopicName());

        // If the RETAIN flag is set to 1, in a PUBLISH Packet sent by a Client to a Server, the Server MUST store
        // the Application Message and its QoS, so that it can be delivered to future subscribers whose
        // subscriptions match its topic name. When a new subscription is established, the last
        // retained message, if any, on each matching topic name MUST be sent to the subscriber.
        if (msg.isRetain()) {
            // If the Server receives a QoS 0 message with the RETAIN flag set to 1 it MUST discard any message
            // previously retained for that topic. It SHOULD store the new QoS 0 message as the new retained
            // message for that topic, but MAY choose to discard it at any time - if this happens there will be no retained
            // message for that topic.
            if (msg.getQos() == MqttQoS.AT_MOST_ONCE || msg.getPayload().getPayload() == null || msg.getPayload().getPayload().length == 0) {
                logger.trace("Clear retain: Clear retain messages for topic {} by client {}", msg.getPayload().getTopicName(), msg.getClientId());
                this.redis.removeAllRetainMessage(topicLevels);
            }

            // A PUBLISH Packet with a RETAIN flag set to 1 and a payload containing zero bytes will be processed as
            // normal by the Server and sent to Clients with a subscription matching the topic name. Additionally any
            // existing retained message with the same topic name MUST be removed and any future subscribers for
            // the topic will not receive a retained message. “As normal” means that the RETAIN flag is
            // not set in the message received by existing Clients. A zero byte retained message MUST NOT be stored
            // as a retained message on the Server
            if (msg.getPayload().getPayload() != null && msg.getPayload().getPayload().length > 0) {
                logger.trace("Add retain: Add retain messages for topic {} by client {}", msg.getPayload().getTopicName(), msg.getClientId());
                this.redis.addRetainMessage(topicLevels, msg);
            }
        }

        // In the QoS 0 delivery protocol, the Receiver
        // Accepts ownership of the message when it receives the PUBLISH packet.
        if (msg.getQos() == MqttQoS.AT_MOST_ONCE) {
            onwardRecipients(msg);
        }
        // In the QoS 1 delivery protocol, the Receiver
        // After it has sent a PUBACK Packet the Receiver MUST treat any incoming PUBLISH packet that
        // contains the same Packet Identifier as being a new publication, irrespective of the setting of its
        // DUP flag.
        else if (msg.getQos() == MqttQoS.AT_LEAST_ONCE) {
            onwardRecipients(msg);
        }
        // In the QoS 2 delivery protocol, the Receiver
        // Until it has received the corresponding PUBREL packet, the Receiver MUST acknowledge any
        // subsequent PUBLISH packet with the same Packet Identifier by sending a PUBREC. It MUST
        // NOT cause duplicate messages to be delivered to any onward recipients in this case.
        else if (msg.getQos() == MqttQoS.EXACTLY_ONCE) {
            // The recipient of a Control Packet that contains the DUP flag set to 1 cannot assume that it has
            // seen an earlier copy of this packet.
            if (this.redis.addQoS2MessageId(msg.getClientId(), msg.getPayload().getPacketId())) {
                onwardRecipients(msg);
            }
        }
    }

    /**
     * Forward PUBLISH message to its recipients
     *
     * @param msg Internal PUBLISH Message
     */
    protected void onwardRecipients(InternalMessage<Publish> msg) {
        List<String> topicLevels = Topics.sanitizeTopicName(msg.getPayload().getTopicName());

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
        subscriptions.forEach((clientId, qos) -> {

            // Compare publish QoS and subscription QoS
            MqttQoS fQos = msg.getQos().value() > qos.value() ? qos : msg.getQos();

            // Each time a Client sends a new packet of one of these
            // types it MUST assign it a currently unused Packet Identifier. If a Client re-sends a
            // particular Control Packet, then it MUST use the same Packet Identifier in subsequent re-sends of that
            // packet. The Packet Identifier becomes available for reuse after the Client has processed the
            // corresponding acknowledgement packet. In the case of a QoS 1 PUBLISH this is the corresponding
            // PUBACK; in the case of QoS 2 it is PUBCOMP. For SUBSCRIBE or UNSUBSCRIBE it is the
            // corresponding SUBACK or UNSUBACK. The same conditions apply to a Server when it
            // sends a PUBLISH with QoS > 0
            // A PUBLISH Packet MUST NOT contain a Packet Identifier if its QoS value is set to
            int packetId = 0;
            if (fQos == MqttQoS.AT_LEAST_ONCE || fQos == MqttQoS.EXACTLY_ONCE) {
                packetId = this.redis.getNextPacketId(clientId);
            }
            Publish p = new Publish(msg.getPayload().getTopicName(), packetId, msg.getPayload().getPayload());
            InternalMessage<Publish> m = new InternalMessage<>(MqttMessageType.PUBLISH, false, fQos, false,
                    MqttVersion.MQTT_3_1_1, false, clientId, null, null, p);

            // Forward to recipient
            String brokerId = this.redis.getConnectedNode(clientId);
            boolean dup = false;
            if (StringUtils.isNotBlank(brokerId)) {
                logger.trace("Communicator sending: Send PUBLISH message to broker {} for client {} subscription", brokerId, clientId);
                dup = true;
                this.communicator.sendToBroker(brokerId, m);
            }

            // In the QoS 1 delivery protocol, the Sender
            // MUST treat the PUBLISH Packet as “unacknowledged” until it has received the corresponding
            // PUBACK packet from the receiver.
            // In the QoS 2 delivery protocol, the Sender
            // MUST treat the PUBLISH packet as “unacknowledged” until it has received the corresponding
            // PUBREC packet from the receiver.
            if (fQos == MqttQoS.AT_LEAST_ONCE || fQos == MqttQoS.EXACTLY_ONCE) {
                logger.trace("Add in-flight: Add in-flight PUBLISH message {} with QoS {} for client {}", packetId, fQos, clientId);
                this.redis.addInFlightMessage(clientId, packetId, m, dup);
            }
        });
    }

    @Override
    public void onSubscribe(InternalMessage<Subscribe> msg) {
        msg.getPayload().getSubscriptions().forEach(subscription -> {
            // Granted only
            if (subscription.getGrantedQos() != MqttSubAckReturnCode.FAILURE) {

                // Sanitize to topic levels
                List<String> topicLevels = Topics.sanitize(subscription.getTopic());

                // If a Server receives a SUBSCRIBE Packet containing a Topic Filter that is identical to an existing
                // Subscription’s Topic Filter then it MUST completely replace that existing Subscription with a new
                // Subscription. The Topic Filter in the new Subscription will be identical to that in the previous Subscription,
                // although its maximum QoS value could be different. Any existing retained messages matching the Topic
                // Filter MUST be re-sent, but the flow of publications MUST NOT be interrupted.
                // Where the Topic Filter is not identical to any existing Subscription’s filter, a new Subscription is created
                // and all matching retained messages are sent.
                logger.trace("Update subscription: Update client {} subscription with topic {} QoS {}", msg.getClientId(), subscription.getTopic(), subscription.getGrantedQos());
                this.redis.updateSubscription(msg.getClientId(), topicLevels, MqttQoS.valueOf(subscription.getGrantedQos().value()));

                // The Server is permitted to start sending PUBLISH packets matching the Subscription before the Server
                // sends the SUBACK Packet.
                for (InternalMessage<Publish> retain : this.redis.getAllRetainMessages(topicLevels)) {

                    // Set recipient client id
                    retain.setClientId(msg.getClientId());

                    // Compare publish QoS and subscription QoS
                    if (retain.getQos().value() > subscription.getGrantedQos().value()) {
                        retain.setQos(MqttQoS.valueOf(subscription.getGrantedQos().value()));
                    }

                    // Set packet id
                    if (retain.getQos() == MqttQoS.AT_LEAST_ONCE || retain.getQos() == MqttQoS.EXACTLY_ONCE) {
                        int packetId = this.redis.getNextPacketId(msg.getClientId());
                        retain.getPayload().setPacketId(packetId);
                    }

                    // Forward to recipient
                    logger.trace("Communicator sending: Send retained PUBLISH message to broker {} for client {} subscription with topic {}", msg.getBrokerId(), msg.getClientId(), subscription.getTopic());
                    this.communicator.sendToBroker(msg.getBrokerId(), retain);

                    // In the QoS 1 delivery protocol, the Sender
                    // MUST treat the PUBLISH Packet as “unacknowledged” until it has received the corresponding
                    // PUBACK packet from the receiver.
                    // In the QoS 2 delivery protocol, the Sender
                    // MUST treat the PUBLISH packet as “unacknowledged” until it has received the corresponding
                    // PUBREC packet from the receiver.
                    if (retain.getQos() == MqttQoS.AT_LEAST_ONCE || retain.getQos() == MqttQoS.EXACTLY_ONCE) {
                        logger.trace("Add in-flight: Add in-flight PUBLISH message {} with QoS {} for client {}", retain.getPayload().getPacketId(), retain.getQos(), retain.getClientId());
                        this.redis.addInFlightMessage(retain.getClientId(), retain.getPayload().getPacketId(), retain, true);
                    }
                }
            }
        });
    }

    @Override
    public void onUnsubscribe(InternalMessage<Unsubscribe> msg) {
        // The Topic Filters (whether they contain wildcards or not) supplied in an UNSUBSCRIBE packet MUST be
        // compared character-by-character with the current set of Topic Filters held by the Server for the Client. If
        // any filter matches exactly then its owning Subscription is deleted, otherwise no additional processing
        // occurs
        // If a Server deletes a Subscription:
        // It MUST stop adding any new messages for delivery to the Client.
        //1 It MUST complete the delivery of any QoS 1 or QoS 2 messages which it has started to send to
        // the Client.
        // It MAY continue to deliver any existing messages buffered for delivery to the Client.
        msg.getPayload().getTopics().forEach(topic -> {
            logger.trace("Remove subscription: Remove client {} subscription with topic {}", msg.getClientId(), topic);
            this.redis.removeSubscription(msg.getClientId(), Topics.sanitize(topic));
        });
    }

    @Override
    public void onDisconnect(InternalMessage<Disconnect> msg) {
        // Test if client already reconnected to another broker
        if (msg.getBrokerId().equals(this.redis.getConnectedNode(msg.getClientId()))) {

            // If CleanSession is set to 1, the Client and Server MUST discard any previous Session and start a new
            // one. This Session lasts as long as the Network Connection. State data associated with this Session
            // MUST NOT be reused in any subsequent Session.
            // When CleanSession is set to 1 the Client and Server need not process the deletion of state atomically.
            if (msg.isCleanSession()) {
                logger.trace("Clear session: Clear session state for client {} because current connection is clean session", msg.getClientId());
                this.redis.removeAllSessionState(msg.getClientId());
            }

            // Remove connected node
            logger.trace("Remove node: Mark client {} disconnected from broker {}", msg.getClientId(), msg.getBrokerId());
            this.redis.removeConnectedNode(msg.getClientId(), msg.getBrokerId());
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
    }
}
