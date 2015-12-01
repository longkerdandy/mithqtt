package com.github.longkerdandy.mithril.mqtt.communicator.hazelcast.application;

import com.github.longkerdandy.mithril.mqtt.api.comm.ApplicationListener;
import com.github.longkerdandy.mithril.mqtt.api.internal.*;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application Communicator Worker for Kafka
 */
public class HazelcastApplicationWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastApplicationWorker.class);

    private final Ringbuffer<InternalMessage> applicationRing;
    private final ApplicationListener listener;

    public HazelcastApplicationWorker(Ringbuffer<InternalMessage> applicationRing, ApplicationListener listener) {
        this.applicationRing = applicationRing;
        this.listener = listener;
    }

    @Override
    @SuppressWarnings({"unchecked", "InfiniteLoopStatement"})
    public void run() {
        try {
            long sequence = this.applicationRing.headSequence();

            while (true) {
                // read message, blocking if no new message
                InternalMessage msg = this.applicationRing.readOne(sequence);

                // notify listener
                if (msg != null) {
                    logger.debug("Communicator received: Received {} message from broker {} for client {} user {}", msg.getMessageType(), msg.getBrokerId(), msg.getClientId(), msg.getUserName());
                    switch (msg.getMessageType()) {
                        case CONNECT:
                            this.listener.onConnect((InternalMessage<Connect>) msg);
                            break;
                        case PUBLISH:
                            this.listener.onPublish((InternalMessage<Publish>) msg);
                            break;
                        case SUBSCRIBE:
                            this.listener.onSubscribe((InternalMessage<Subscribe>) msg);
                            break;
                        case UNSUBSCRIBE:
                            this.listener.onUnsubscribe((InternalMessage<Unsubscribe>) msg);
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
            logger.warn("Communicator error: Interrupted while reading from application ring buffer", e);
        }
    }
}
