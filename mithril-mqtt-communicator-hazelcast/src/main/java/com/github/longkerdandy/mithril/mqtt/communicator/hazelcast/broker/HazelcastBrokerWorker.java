package com.github.longkerdandy.mithril.mqtt.communicator.hazelcast.broker;

import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerListener;
import com.github.longkerdandy.mithril.mqtt.api.internal.Disconnect;
import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.api.internal.Publish;
import com.hazelcast.core.IQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker Communicator Worker for Hazelcast
 */
public class HazelcastBrokerWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastBrokerWorker.class);

    private final IQueue<InternalMessage> brokerQueue;
    private final BrokerListener listener;

    public HazelcastBrokerWorker(IQueue<InternalMessage> brokerQueue, BrokerListener listener) {
        this.brokerQueue = brokerQueue;
        this.listener = listener;
    }

    @Override
    @SuppressWarnings({"unchecked", "InfiniteLoopStatement"})
    public void run() {
        try {
            while (true) {
                // read message, blocking if no new message
                InternalMessage msg = this.brokerQueue.take();

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
            }
        } catch (InterruptedException e) {
            logger.warn("Communicator error: Interrupted while reading from broker queue", e);
        }
    }
}
