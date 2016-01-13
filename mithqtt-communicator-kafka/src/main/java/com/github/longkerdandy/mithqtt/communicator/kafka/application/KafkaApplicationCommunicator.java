package com.github.longkerdandy.mithqtt.communicator.kafka.application;

import com.github.longkerdandy.mithqtt.api.comm.ApplicationListenerFactory;
import com.github.longkerdandy.mithqtt.communicator.kafka.KafkaCommunicator;
import com.github.longkerdandy.mithqtt.api.comm.ApplicationCommunicator;
import com.github.longkerdandy.mithqtt.communicator.kafka.codec.InternalMessageSerializer;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Application Communicator implementation for Kafka
 */
@SuppressWarnings("unused")
public class KafkaApplicationCommunicator extends KafkaCommunicator implements ApplicationCommunicator {

    private static final Logger logger = LoggerFactory.getLogger(KafkaApplicationCommunicator.class);

    private KafkaApplicationWorker worker;

    @Override
    public void init(AbstractConfiguration config, ApplicationListenerFactory factory) {
        init(config);

        logger.trace("Initializing Kafka consumer ...");

        // consumer config
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getString("bootstrap.servers"));
        props.put("group.id", config.getString("group.id"));
        props.put("enable.auto.commit", "true");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", InternalMessageSerializer.class.getName());

        // consumer
        this.consumer = new KafkaConsumer<>(props);

        // consumer worker
        this.worker = new KafkaApplicationWorker(this.consumer, APPLICATION_TOPIC, factory.newListener());
        this.executor.submit(this.worker);
    }

    @Override
    public void destroy() {
        // shutdown worker
        this.worker.closed.set(true);
        this.consumer.wakeup();

        super.destroy();
    }
}
