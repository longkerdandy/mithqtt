package com.github.longkerdandy.mithril.mqtt.processor.comm;

import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorListener;
import com.github.longkerdandy.mithril.mqtt.api.internal.*;
import com.github.longkerdandy.mithril.mqtt.storage.redis.RedisSyncStorage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processor Listener Implementation
 */
public class ProcessorListenerImpl implements ProcessorListener {

    private static final Logger logger = LoggerFactory.getLogger(ProcessorListener.class);

    private ProcessorCommunicator communicator;
    private RedisSyncStorage redis;

    @Override
    public void onConnect(InternalMessage<Connect> msg) {
        // first, mark client's connected broker node
        String previous = this.redis.updateConnectedNode(msg.getClientId(), msg.getBrokerId());

        // If the ClientId represents a Client already connected to the Server then the Server MUST
        // disconnect the existing Client
        if (StringUtils.isNotBlank(previous) && !previous.equals(msg.getBrokerId())) {
            InternalMessage<Disconnect> m = new InternalMessage<>();
            m.setMessageType(MqttMessageType.DISCONNECT);
            m.setQos(MqttQoS.AT_MOST_ONCE);
            m.setVersion(MqttVersion.MQTT_3_1_1);
            m.setClientId(msg.getClientId());
            Disconnect d = new Disconnect();
            d.setClearExit(false);
            m.setPayload(d);
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
                for (InternalMessage inFlight : this.redis.getAllInFlightMessages(msg.getClientId())) {
                    communicator.sendToBroker(msg.getBrokerId(), inFlight);
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

        // last, mark client's session as existed
        this.redis.updateSessionExist(msg.getClientId(), msg.isCleanSession());
    }

    @Override
    public void onPublish(InternalMessage<Publish> msg) {

    }

    @Override
    public void onSubscribe(InternalMessage<Subscribe> msg) {

    }

    @Override
    public void onUnsubscribe(InternalMessage<Unsubscribe> msg) {

    }

    @Override
    public void onDisconnect(InternalMessage msg) {

    }
}
