package com.github.longkerdandy.mithqtt.communicator.kafka;

import com.github.longkerdandy.mithqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithqtt.communicator.kafka.codec.InternalMessageSerializer;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    protected String BROKER_TOPIC_PREFIX;
    protected String APPLICATION_TOPIC;

    protected KafkaProducer<String, InternalMessage> producer;
    protected KafkaConsumer<String, InternalMessage> consumer;
    protected ExecutorService executor;

    protected void init(AbstractConfiguration config) {
        BROKER_TOPIC_PREFIX = config.getString("communicator.broker.topic");
        APPLICATION_TOPIC = config.getString("communicator.application.topic");

        logger.trace("Initializing Kafka producer ...");

        // producer config
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getString("bootstrap.servers"));
        props.put("acks", config.getString("acks"));
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", InternalMessageSerializer.class.getName());

        // producer
        this.producer = new KafkaProducer<>(props);

        // consumer executor
        this.executor = Executors.newSingleThreadExecutor();
    }

    public void destroy() {
        if (this.producer != null) this.producer.close();
        if (this.consumer != null) this.consumer.close();
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

    public void sendToApplication(InternalMessage message) {
        sendToTopic(APPLICATION_TOPIC, message);
    }

    protected void sendToTopic(String topic, InternalMessage message) {
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
