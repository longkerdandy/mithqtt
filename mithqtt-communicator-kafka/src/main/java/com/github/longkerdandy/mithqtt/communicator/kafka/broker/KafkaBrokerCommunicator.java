package com.github.longkerdandy.mithqtt.communicator.kafka.broker;

import com.github.longkerdandy.mithqtt.communicator.kafka.KafkaCommunicator;
import com.github.longkerdandy.mithqtt.util.UUIDs;
import com.github.longkerdandy.mithqtt.api.comm.BrokerCommunicator;
import com.github.longkerdandy.mithqtt.api.comm.BrokerListenerFactory;
import com.github.longkerdandy.mithqtt.communicator.kafka.codec.InternalMessageSerializer;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.github.longkerdandy.mithqtt.util.UUIDs.shortUuid;

/**
 * Broker Communicator implementation for Kafka
 */
@SuppressWarnings("unused")
public class KafkaBrokerCommunicator extends KafkaCommunicator implements BrokerCommunicator {

    private static final Logger logger = LoggerFactory.getLogger(KafkaBrokerCommunicator.class);

    protected static String BROKER_TOPIC;

    private KafkaBrokerWorker worker;

    @Override
    public void init(AbstractConfiguration config, String brokerId, BrokerListenerFactory factory) {
        init(config);

        BROKER_TOPIC = BROKER_TOPIC_PREFIX + "." + brokerId;

        logger.trace("Initializing Kafka consumer ...");

        // consumer config
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getString("bootstrap.servers"));
        props.put("group.id", UUIDs.shortUuid());
        props.put("enable.auto.commit", "true");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", InternalMessageSerializer.class.getName());

        // consumer
        this.consumer = new KafkaConsumer<>(props);

        // consumer worker
        this.worker = new KafkaBrokerWorker(this.consumer, BROKER_TOPIC, factory.newListener());
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
