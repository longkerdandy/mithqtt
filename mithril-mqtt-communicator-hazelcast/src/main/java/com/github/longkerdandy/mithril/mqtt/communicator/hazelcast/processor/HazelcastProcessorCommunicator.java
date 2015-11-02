package com.github.longkerdandy.mithril.mqtt.communicator.hazelcast.processor;

import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorListenerFactory;
import com.github.longkerdandy.mithril.mqtt.communicator.hazelcast.HazelcastCommunicator;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Processor Communicator implementation for Hazelcast
 */
@SuppressWarnings("unused")
public class HazelcastProcessorCommunicator extends HazelcastCommunicator implements ProcessorCommunicator {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastProcessorCommunicator.class);

    private ExecutorService executor;

    @Override
    public void init(PropertiesConfiguration config, ProcessorListenerFactory factory) {
        init(config);

        logger.trace("Initializing Hazelcast processor consumer's workers ...");

        // consumer executor
        int threads = config.getInt("consumer.threads");
        this.executor = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            this.executor.submit(new HazelcastProcessorWorker(this.processorQueue, factory.newListener()));
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
