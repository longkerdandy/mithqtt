package com.github.longkerdandy.mithqtt.communicator.hazelcast.http;

import com.github.longkerdandy.mithqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithqtt.api.comm.HttpCommunicator;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Http Communicator implementation for Hazelcast
 */
@SuppressWarnings("unused")
public class HazelcastHttpCommunicator implements HttpCommunicator {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastHttpCommunicator.class);

    // hazelcast instance
    protected HazelcastInstance hazelcast;

    // topics
    protected String BROKER_TOPIC_PREFIX;
    protected String APPLICATION_TOPIC;

    @Override
    public void init(AbstractConfiguration config, String serverId) {
        this.hazelcast = Hazelcast.newHazelcastInstance();

        logger.trace("Initializing Hazelcast broker resources ...");

        BROKER_TOPIC_PREFIX = config.getString("communicator.broker.topic");
        APPLICATION_TOPIC = config.getString("communicator.application.topic");
    }

    @Override
    public void destroy() {
        if (this.hazelcast != null) this.hazelcast.shutdown();
    }

    @Override
    public void sendToBroker(String brokerId, InternalMessage message) {
        Ringbuffer<InternalMessage> ring = this.hazelcast.getRingbuffer(BROKER_TOPIC_PREFIX + "." + brokerId);
        sendMessage(ring, message);
    }

    @Override
    public void sendToApplication(InternalMessage message) {
        Ringbuffer<InternalMessage> ring = this.hazelcast.getRingbuffer(APPLICATION_TOPIC);
        sendMessage(ring, message);
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
