package com.github.longkerdandy.mithril.mqtt.communicator.hazelcast.broker;

import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerListenerFactory;
import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.communicator.hazelcast.HazelcastCommunicator;
import com.hazelcast.core.IQueue;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Processor Communicator implementation for Hazelcast
 */
public class HazelcastBrokerCommunicator extends HazelcastCommunicator implements BrokerCommunicator {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastBrokerCommunicator.class);

    private ExecutorService executor;

    @Override
    public void init(PropertiesConfiguration config, String brokerId, BrokerListenerFactory factory) {
        init(config);


        logger.trace("Initializing Hazelcast broker RingBuffer ...");

        IQueue<InternalMessage> brokerQueue = this.hazelcast.getQueue(BROKER_TOPIC_PREFIX + "." + brokerId);

        logger.trace("Initializing Hazelcast broker consumer's workers ...");

        // consumer executor
        int threads = config.getInt("consumer.threads");
        this.executor = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < 10; i++) {
            this.executor.submit(new HazelcastBrokerWorker(brokerQueue, factory.newListener()));
        }
    }

    @Override
    public void destroy() {
        super.destroy();

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
}
