package com.github.longkerdandy.mithril.mqtt.communicator.hazelcast;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Communicator implementation based on Hazelcast
 */
public class HazelcastCommunicator {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastCommunicator.class);

    // hazelcast instance
    protected HazelcastInstance hazelcast;

    // broker
    protected String BROKER_TOPIC_PREFIX;

    // processor
    protected IQueue<InternalMessage> processorQueue;

    // application
    protected IQueue<InternalMessage> applicationQueue;

    protected void init(PropertiesConfiguration config) {
        this.hazelcast = Hazelcast.newHazelcastInstance();

        logger.trace("Initializing Hazelcast broker resources ...");

        BROKER_TOPIC_PREFIX = config.getString("communicator.broker.topic");

        logger.trace("Initializing Hazelcast processor resources ...");

        this.processorQueue = this.hazelcast.getQueue(config.getString("communicator.processor.topic"));

        logger.trace("Initializing Hazelcast application resources ...");

        this.applicationQueue = this.hazelcast.getQueue(config.getString("communicator.application.topic"));
    }

    protected void destroy() {
        if (this.hazelcast != null) this.hazelcast.shutdown();
    }

    public void sendToBroker(String brokerId, InternalMessage message) {
        IQueue<InternalMessage> brokerQueue = this.hazelcast.getQueue(BROKER_TOPIC_PREFIX + "." + brokerId);
        sendMessage(brokerQueue, message);
    }

    public void sendToProcessor(InternalMessage message) {
        sendMessage(this.processorQueue, message);
    }

    public void sendToApplication(InternalMessage message) {
        sendMessage(this.applicationQueue, message);
    }

    protected void sendMessage(IQueue<InternalMessage> queue, InternalMessage message) {
        if (queue.offer(message)) {
            logger.debug("Communicator succeed: Successful add message {} to queue {}", message.getMessageType(), queue.getName());
        } else {
            logger.warn("Communicator failed: Failed to add message {} to queue {}: Operation timeour", message.getMessageType(), queue.getName());
        }

    }
}
