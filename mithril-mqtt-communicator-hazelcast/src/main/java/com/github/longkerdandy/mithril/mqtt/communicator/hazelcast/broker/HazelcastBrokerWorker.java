package com.github.longkerdandy.mithril.mqtt.communicator.hazelcast.broker;

import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerListener;
import com.github.longkerdandy.mithril.mqtt.api.internal.Disconnect;
import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.api.internal.Publish;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.StaleSequenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker Communicator Worker for Hazelcast
 */
public class HazelcastBrokerWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastBrokerWorker.class);

    private final Ringbuffer<InternalMessage> brokerBuffer;
    private final IAtomicLong brokerSeq;
    private final BrokerListener listener;

    public HazelcastBrokerWorker(Ringbuffer<InternalMessage> brokerBuffer, IAtomicLong brokerSeq, BrokerListener listener) {
        this.brokerBuffer = brokerBuffer;
        this.brokerSeq = brokerSeq;
        this.listener = listener;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
        try {
            while (true) {
                // global sequence counter
                long seq = this.brokerSeq.getAndIncrement();

                try {
                    // read message, blocking if no new message
                    InternalMessage msg = this.brokerBuffer.readOne(seq);

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
                } catch (StaleSequenceException e) {
                    logger.warn("Communicator warn: Sequence {} not exist in broker RingBuffer: ", seq, e);
                }
            }
        } catch (InterruptedException e) {
            logger.warn("Communicator error: Interrupted while reading from broker RingBuffer", e);
        }
    }
}
