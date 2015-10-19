package com.github.longkerdandy.mithril.mqtt.communicator.kafka.processor;

import com.github.longkerdandy.mithril.mqtt.api.comm.Communicators;
import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorListenerFactory;
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
 * Processor Communicator implementation for Kafka
 */
@SuppressWarnings("unused")
public class KafkaProcessorCommunicator extends KafkaCommunicator implements ProcessorCommunicator {

    @Override
    public void init(PropertiesConfiguration config, ProcessorListenerFactory factory) {
        init(config);

        // consumer connect to kafka
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(Communicators.PROCESSOR, config.getInt("consumer.threads"));
        Map<String, List<KafkaStream<String, InternalMessage>>> consumerMap = this.consumer.createMessageStreams(topicCountMap, new StringDecoder(null), new InternalMessageDecoder());
        List<KafkaStream<String, InternalMessage>> streams = consumerMap.get(Communicators.PROCESSOR);

        // launch all consumer workers
        for (final KafkaStream<String, InternalMessage> stream : streams) {
            this.executor.submit(new KafkaProcessorWorker(stream, factory.newListener(this)));
        }
    }
}
