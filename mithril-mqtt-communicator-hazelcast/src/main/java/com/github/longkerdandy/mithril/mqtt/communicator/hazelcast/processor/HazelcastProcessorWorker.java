package com.github.longkerdandy.mithril.mqtt.communicator.hazelcast.processor;

import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorListener;
import com.github.longkerdandy.mithril.mqtt.api.internal.*;
import com.hazelcast.core.IQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processor Communicator Worker for Hazelcast
 */
public class HazelcastProcessorWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastProcessorWorker.class);

    private final IQueue<InternalMessage> processorQueue;
    private final ProcessorListener listener;

    public HazelcastProcessorWorker(IQueue<InternalMessage> processorQueue, ProcessorListener listener) {
        this.processorQueue = processorQueue;
        this.listener = listener;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
        try {
            while (true) {
                // read message, blocking if no new message
                InternalMessage msg = this.processorQueue.take();

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
            }
        } catch (InterruptedException e) {
            logger.warn("Communicator error: Interrupted while reading from processor queue", e);
        }
    }
}
