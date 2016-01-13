package com.github.longkerdandy.mithqtt.communicator.hazelcast.broker;

import com.github.longkerdandy.mithqtt.api.comm.BrokerCommunicator;
import com.github.longkerdandy.mithqtt.api.comm.BrokerListenerFactory;
import com.github.longkerdandy.mithqtt.api.internal.InternalMessage;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Broker Communicator implementation for Hazelcast
 */
@SuppressWarnings("unused")
public class HazelcastBrokerCommunicator implements BrokerCommunicator {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastBrokerCommunicator.class);

    // hazelcast instance
    protected HazelcastInstance hazelcast;

    // broker
    protected String BROKER_TOPIC_PREFIX;
    protected Ringbuffer<InternalMessage> brokerRing;

    // application
    protected Ringbuffer<InternalMessage> applicationRing;

    // executor
    private ExecutorService executor;

    @Override
    public void init(AbstractConfiguration config, String brokerId, BrokerListenerFactory factory) {
        this.hazelcast = Hazelcast.newHazelcastInstance();

        logger.trace("Initializing Hazelcast broker resources ...");

        BROKER_TOPIC_PREFIX = config.getString("communicator.broker.topic");
        this.brokerRing = this.hazelcast.getRingbuffer(BROKER_TOPIC_PREFIX + "." + brokerId);

        logger.trace("Initializing Hazelcast application resources ...");

        this.applicationRing = this.hazelcast.getRingbuffer(config.getString("communicator.application.topic"));

        logger.trace("Initializing Hazelcast broker consumer's workers ...");

        // consumer executor
        this.executor = Executors.newSingleThreadExecutor();
        this.executor.submit(new HazelcastBrokerWorker(this.brokerRing, factory.newListener()));
    }

    @Override
    public void destroy() {
        if (this.brokerRing != null) this.brokerRing.destroy();
        if (this.hazelcast != null) this.hazelcast.shutdown();
        if (this.executor != null) {
            this.executor.shutdown();
            try {
                if (!this.executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    logger.warn("Communicator error: Timed out waiting for consumer threads to shut down, exiting uncleanly");
                }
            } catch (InterruptedException e) {
                logger.warn("Communicator error: Interrupted during shutdown, exiting uncleanly");
            }
        }
    }

    public void sendToBroker(String brokerId, InternalMessage message) {
        Ringbuffer<InternalMessage> ring = this.hazelcast.getRingbuffer(BROKER_TOPIC_PREFIX + "." + brokerId);
        sendMessage(ring, message);
    }

    public void sendToApplication(InternalMessage message) {
        sendMessage(this.applicationRing, message);
    }

    /**
     * Send internal message to hazelcast ring
     *
     * @param ring    Hazelcast RingBuffer
     * @param message Internal Message
     */
    protected void sendMessage(Ringbuffer<InternalMessage> ring, InternalMessage message) {
        ring.addAsync(message, OverflowPolicy.OVERWRITE).andThen(new ExecutionCallback<Long>() {
            @Override
            public void onResponse(Long response) {
                if (response > 0) {
                    logger.debug("Communicator succeed: Successful add message {} to ring buffer {}", message.getMessageType(), ring.getName());
                } else {
                    logger.debug("Communicator failed: Failed to add message {} to ring buffer {}: no space", message.getMessageType(), ring.getName());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                logger.warn("Communicator failed: Failed to add message {} to ring buffer {}: ", message.getMessageType(), ring.getName(), t);
            }
        });
    }
}
