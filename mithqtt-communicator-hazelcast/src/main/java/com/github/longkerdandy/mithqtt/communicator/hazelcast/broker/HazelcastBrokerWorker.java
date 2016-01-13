package com.github.longkerdandy.mithqtt.communicator.hazelcast.broker;

import com.github.longkerdandy.mithqtt.api.comm.BrokerListener;
import com.github.longkerdandy.mithqtt.api.internal.Disconnect;
import com.github.longkerdandy.mithqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithqtt.api.internal.Publish;
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
            // always read new messages only
            long sequence = this.brokerRing.tailSequence() + 1;

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
            logger.error("Communicator error: Interrupted while reading from broker ring buffer", e);
        } catch (Exception e) {
            logger.error("Communicator error: Unknown error while reading from application ring buffer", e);
        }
    }
}
