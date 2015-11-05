package com.github.longkerdandy.mithril.mqtt.communicator.hazelcast.application;

import com.github.longkerdandy.mithril.mqtt.api.comm.ApplicationCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.ApplicationListenerFactory;
import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Application Communicator implementation for Hazelcast
 */
@SuppressWarnings("unused")
public class HazelcastApplicationCommunicator implements ApplicationCommunicator {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastApplicationCommunicator.class);

    // hazelcast client instance
    protected HazelcastInstance hazelcast;

    // application
    protected IQueue<InternalMessage> applicationQueue;

    // executor
    private ExecutorService executor;

    @Override
    public void init(PropertiesConfiguration config, ApplicationListenerFactory factory) {
        this.hazelcast = HazelcastClient.newHazelcastClient();

        logger.trace("Initializing Hazelcast application resources ...");

        this.applicationQueue = this.hazelcast.getQueue(config.getString("communicator.application.topic"));

        logger.trace("Initializing Hazelcast application consumer's workers ...");

        // consumer executor
        int threads = config.getInt("consumer.threads");
        this.executor = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            this.executor.submit(new HazelcastApplicationWorker(this.applicationQueue, factory.newListener()));
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
