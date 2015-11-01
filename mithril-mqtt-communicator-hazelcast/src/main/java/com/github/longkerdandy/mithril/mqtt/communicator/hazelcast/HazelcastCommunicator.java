package com.github.longkerdandy.mithril.mqtt.communicator.hazelcast;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
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
    protected Ringbuffer<InternalMessage> processorBuffer;
    protected IAtomicLong processorSeq;

    // application
    protected Ringbuffer<InternalMessage> applicationBuffer;
    protected IAtomicLong applicationSeq;

    protected void init(PropertiesConfiguration config) {
        this.hazelcast = Hazelcast.newHazelcastInstance();

        BROKER_TOPIC_PREFIX = config.getString("communicator.broker.topic");

        logger.trace("Initializing Hazelcast processor resources ...");

        this.processorBuffer = this.hazelcast.getRingbuffer(config.getString("communicator.processor.topic"));
        this.processorSeq = this.hazelcast.getAtomicLong(config.getString("communicator.processor.topic") + ".seq");

        logger.trace("Initializing Hazelcast application resources ...");

        this.applicationBuffer = this.hazelcast.getRingbuffer(config.getString("communicator.application.topic"));
        this.applicationSeq = this.hazelcast.getAtomicLong(config.getString("communicator.application.topic") + ".seq");
    }

    protected void destroy() {
        if (this.hazelcast != null) this.hazelcast.shutdown();
    }

    public void sendToBroker(String brokerId, InternalMessage message) {
        Ringbuffer<InternalMessage> brokerBuffer = this.hazelcast.getRingbuffer(BROKER_TOPIC_PREFIX + "." + brokerId);
        sendMessage(brokerBuffer, message);
    }

    public void sendToProcessor(InternalMessage message) {
        sendMessage(this.processorBuffer, message);
    }

    public void sendToApplication(InternalMessage message) {
        sendMessage(this.applicationBuffer, message);
    }

    protected void sendMessage(Ringbuffer<InternalMessage> ringBuffer, InternalMessage message) {
        ringBuffer.addAsync(message, OverflowPolicy.OVERWRITE).andThen(new ExecutionCallback<Long>() {
            @Override
            public void onResponse(Long sequence) {
                if (sequence < 0) {
                    logger.warn("Communicator failed: Topic {} is full, message {} has been dropped", ringBuffer.getName(), message.getMessageType());
                } else {
                    logger.debug("Communicator succeed: Successful send message {} to topic {} at sequence {}", message.getMessageType(), ringBuffer.getName(), sequence);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                logger.error("Communicator failed: Failed to send message {} to topic {}: ", message.getMessageType(), ringBuffer.getName(), t);
            }
        });
    }
}
