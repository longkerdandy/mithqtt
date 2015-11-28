package com.github.longkerdandy.mithril.mqtt.communicator.hazelcast.broker;

import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerListenerFactory;
import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Processor Communicator implementation for Hazelcast
 */
@SuppressWarnings("unused")
public class HazelcastBrokerCommunicator implements BrokerCommunicator {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastBrokerCommunicator.class);

    // hazelcast instance
    protected HazelcastInstance hazelcast;

    // broker
    protected String BROKER_TOPIC_PREFIX;
    protected IQueue<InternalMessage> brokerQueue;

    // application
    protected IQueue<InternalMessage> applicationQueue;

    // executor
    private ExecutorService executor;

    @Override
    public void init(AbstractConfiguration config, String brokerId, BrokerListenerFactory factory) {
        this.hazelcast = Hazelcast.newHazelcastInstance();

        logger.trace("Initializing Hazelcast broker resources ...");

        BROKER_TOPIC_PREFIX = config.getString("communicator.broker.topic");
        this.brokerQueue = this.hazelcast.getQueue(BROKER_TOPIC_PREFIX + "." + brokerId);

        logger.trace("Initializing Hazelcast application resources ...");

        this.applicationQueue = this.hazelcast.getQueue(config.getString("communicator.application.topic"));

        logger.trace("Initializing Hazelcast broker consumer's workers ...");

        // consumer executor
        int threads = config.getInt("consumer.threads");
        this.executor = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            this.executor.submit(new HazelcastBrokerWorker(this.brokerQueue, factory.newListener()));
        }
    }

    @Override
    public void destroy() {
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

    @Override
    public void clear() {
        this.brokerQueue.clear();
    }

    public void sendToBroker(String brokerId, InternalMessage message) {
        IQueue<InternalMessage> brokerQueue = this.hazelcast.getQueue(BROKER_TOPIC_PREFIX + "." + brokerId);
        sendMessage(brokerQueue, message);
    }

    public void sendToApplication(InternalMessage message) {
        sendMessage(this.applicationQueue, message);
    }

    /**
     * Send internal message to hazelcast queue
     *
     * @param queue   Hazelcast Queue
     * @param message Internal Message
     */
    protected void sendMessage(IQueue<InternalMessage> queue, InternalMessage message) {
        if (queue.offer(message)) {
            logger.debug("Communicator succeed: Successful add message {} to queue {}", message.getMessageType(), queue.getName());
        } else {
            logger.warn("Communicator failed: Failed to add message {} to queue {}: Operation timeout", message.getMessageType(), queue.getName());
        }

    }
}
