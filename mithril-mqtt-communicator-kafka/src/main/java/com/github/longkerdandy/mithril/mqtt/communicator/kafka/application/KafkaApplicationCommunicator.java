package com.github.longkerdandy.mithril.mqtt.communicator.kafka.application;

import com.github.longkerdandy.mithril.mqtt.api.comm.ApplicationCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.ApplicationListenerFactory;
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
 * Application Communicator implementation for Kafka
 */
@SuppressWarnings("unused")
public class KafkaApplicationCommunicator extends KafkaCommunicator implements ApplicationCommunicator {

    private static final Logger logger = LoggerFactory.getLogger(KafkaApplicationCommunicator.class);

    @Override
    public void init(AbstractConfiguration config, ApplicationListenerFactory factory) {
        init(config);

        logger.trace("Initializing Kafka application consumer and workers ...");

        // consumer connect to kafka
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(APPLICATION_TOPIC, config.getInt("consumer.threads"));
        Map<String, List<KafkaStream<String, InternalMessage>>> consumerMap = this.consumer.createMessageStreams(topicCountMap, new StringDecoder(null), new InternalMessageDecoder());
        List<KafkaStream<String, InternalMessage>> streams = consumerMap.get(APPLICATION_TOPIC);

        // launch all consumer workers
        for (final KafkaStream<String, InternalMessage> stream : streams) {
            this.executor.submit(new KafkaApplicationWorker(stream, factory.newListener()));
        }
    }
}
