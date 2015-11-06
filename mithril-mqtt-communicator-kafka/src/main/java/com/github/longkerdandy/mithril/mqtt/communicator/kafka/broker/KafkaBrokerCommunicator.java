package com.github.longkerdandy.mithril.mqtt.communicator.kafka.broker;

import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerListenerFactory;
import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.communicator.kafka.KafkaCommunicator;
import com.github.longkerdandy.mithril.mqtt.communicator.kafka.codec.InternalMessageDecoder;
import kafka.consumer.KafkaStream;
import kafka.serializer.StringDecoder;
import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Broker Communicator implementation for Kafka
 */
@SuppressWarnings("unused")
public class KafkaBrokerCommunicator extends KafkaCommunicator implements BrokerCommunicator {

    private static final Logger logger = LoggerFactory.getLogger(KafkaBrokerCommunicator.class);

    protected static String BROKER_TOPIC;

    @Override
    public void init(AbstractConfiguration config, String brokerId, BrokerListenerFactory factory) {
        init(config);

        BROKER_TOPIC = BROKER_TOPIC_PREFIX + "." + brokerId;

        logger.trace("Initializing Kafka broker consumer and workers ...");

        // consumer connect to kafka
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(BROKER_TOPIC, config.getInt("consumer.threads"));
        Map<String, List<KafkaStream<String, InternalMessage>>> consumerMap = this.consumer.createMessageStreams(topicCountMap, new StringDecoder(null), new InternalMessageDecoder());
        List<KafkaStream<String, InternalMessage>> streams = consumerMap.get(BROKER_TOPIC);

        // launch all consumer workers
        for (final KafkaStream<String, InternalMessage> stream : streams) {
            this.executor.submit(new KafkaBrokerWorker(stream, factory.newListener()));
        }
    }
}
