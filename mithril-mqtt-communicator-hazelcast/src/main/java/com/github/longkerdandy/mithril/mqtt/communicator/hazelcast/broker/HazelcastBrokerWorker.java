package com.github.longkerdandy.mithril.mqtt.communicator.hazelcast.broker;

import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerListener;
import com.github.longkerdandy.mithril.mqtt.api.internal.Disconnect;
import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.api.internal.Publish;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker Communicator Worker for Hazelcast
 */
public class HazelcastBrokerWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastBrokerWorker.class);

    private final Ringbuffer<InternalMessage> brokerRing;
    private final BrokerListener listener;

    public HazelcastBrokerWorker(Ringbuffer<InternalMessage> brokerRing, BrokerListener listener) {
        this.brokerRing = brokerRing;
        this.listener = listener;
    }

    @Override
    @SuppressWarnings({"unchecked", "InfiniteLoopStatement"})
    public void run() {
        try {
            long sequence = this.brokerRing.headSequence();

            while (true) {
                // read message, blocking if no new message
                InternalMessage msg = this.brokerRing.readOne(sequence);

                // notify listener
                if (msg != null) {
                    logger.debug("Communicator received: Received {} message for client {} user {}", msg.getMessageType(), msg.getClientId(), msg.getUserName());
                    switch (msg.getMessageType()) {
                        case PUBLISH:
                            this.listener.onPublish((InternalMessage<Publish>) msg);
                            break;
                        case DISCONNECT:
                            this.listener.onDisconnect((InternalMessage<Disconnect>) msg);
                            break;
                        default:
                            logger.warn("Communicator error: Communicator received unexpected message type {}", msg.getMessageType());
                    }
                }

                sequence++;
            }
        } catch (InterruptedException e) {
            logger.warn("Communicator error: Interrupted while reading from broker ring buffer", e);
        }
    }
}
