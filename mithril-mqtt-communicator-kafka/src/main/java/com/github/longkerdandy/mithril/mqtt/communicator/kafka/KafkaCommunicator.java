package com.github.longkerdandy.mithril.mqtt.communicator.kafka;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.communicator.kafka.codec.InternalMessageSerializer;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Communicator implementation based on Kafka
 */
@SuppressWarnings("unused")
public abstract class KafkaCommunicator {

    private static final Logger logger = LoggerFactory.getLogger(KafkaCommunicator.class);

    protected static String BROKER_TOPIC_PREFIX;
    protected static String PROCESSOR_TOPIC;
    protected static String BRIDGE_TOPIC;

    protected KafkaProducer<String, InternalMessage> producer;
    protected ConsumerConnector consumer;
    protected ExecutorService executor;

    protected void init(PropertiesConfiguration config) {
        BROKER_TOPIC_PREFIX = config.getString("communicator.broker.topic");
        PROCESSOR_TOPIC = config.getString("communicator.processor.topic");
        BRIDGE_TOPIC = config.getString("communicator.bridge.topic");

        logger.trace("Initializing Kafka producer ...");

        // producer config
        Map<String, Object> map = new HashMap<>();
        map.put("bootstrap.servers", config.getString("bootstrap.servers"));
        map.put("acks", config.getString("acks"));
        map.put("key.serializer", StringSerializer.class.getName());
        map.put("value.serializer", InternalMessageSerializer.class.getName());

        // producer
        this.producer = new KafkaProducer<>(map);

        logger.trace("Initializing Kafka consumer ...");

        // consumer config
        Properties props = new Properties();
        props.put("zookeeper.connect", config.getString("zookeeper.connect"));
        props.put("group.id", config.getString("group.id"));
        ConsumerConfig consumerConfig = new ConsumerConfig(props);

        // consumer
        this.consumer = Consumer.createJavaConsumerConnector(consumerConfig);

        // consumer executor
        this.executor = Executors.newFixedThreadPool(config.getInt("consumer.threads"));
    }

    public void destroy() {
        if (this.producer != null) this.producer.close();
        if (this.consumer != null) this.consumer.shutdown();
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
        sendToTopic(BROKER_TOPIC_PREFIX + "." + brokerId, message);
    }

    public void sendToProcessor(InternalMessage message) {
        sendToTopic(PROCESSOR_TOPIC, message);
    }

    public void sendToBridge(InternalMessage message) {
        sendToTopic(PROCESSOR_TOPIC, message);
    }

    public void sendToTopic(String topic, InternalMessage message) {
        ProducerRecord<String, InternalMessage> record = new ProducerRecord<>(topic, message);
        this.producer.send(record,
                (metadata, e) -> {
                    if (e != null)
                        logger.error("Communicator failed: Failed to send message {} to topic {}: ", message.getMessageType(), topic, e);
                    else {
                        logger.debug("Communicator succeed: Successful send message {} to topic {} partition {} offset {}", message.getMessageType(), topic, metadata.partition(), metadata.offset());
                    }
                });
    }
}
