package com.github.longkerdandy.mithril.mqtt.communicator.kafka.broker;

import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerListenerFactory;
import com.github.longkerdandy.mithril.mqtt.api.comm.Communicators;
import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.communicator.kafka.KafkaCommunicator;
import com.github.longkerdandy.mithril.mqtt.communicator.kafka.codec.InternalMessageDecoder;
import kafka.consumer.KafkaStream;
import kafka.serializer.StringDecoder;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Broker Communicator implementation for Kafka
 */
@SuppressWarnings("unused")
public class KafkaBrokerCommunicator extends KafkaCommunicator implements BrokerCommunicator {

    @Override
    public void init(PropertiesConfiguration config, String brokerId, BrokerListenerFactory factory) {
        init(config);

        // consumer connect to kafka
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(Communicators.BROKER(brokerId), config.getInt("consumer.threads"));
        Map<String, List<KafkaStream<String, InternalMessage>>> consumerMap = this.consumer.createMessageStreams(topicCountMap, new StringDecoder(null), new InternalMessageDecoder());
        List<KafkaStream<String, InternalMessage>> streams = consumerMap.get(Communicators.BROKER(brokerId));

        // launch all consumer workers
        for (final KafkaStream<String, InternalMessage> stream : streams) {
            this.executor.submit(new KafkaBrokerWorker(stream, factory.newListener()));
        }
    }
}
