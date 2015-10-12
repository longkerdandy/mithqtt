package com.github.longkerdandy.mithril.mqtt.communicator.kafka.processor;

import com.github.longkerdandy.mithril.mqtt.api.comm.CommunicatorMessage;
import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorListenerFactory;
import com.github.longkerdandy.mithril.mqtt.communicator.kafka.msg.CommunicatorMessageDecoder;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Communicator implementation based on Kafka for Processor
 */
public class KafkaProcessorCommunicator implements ProcessorCommunicator {

    private ConsumerConnector connector;
    private ExecutorService executor;

    @Override
    public void init(PropertiesConfiguration config, ProcessorListenerFactory factory) {
        // consumer config
        Properties props = new Properties();
        props.put("zookeeper.connect", config.getString("zookeeper.connect"));
        props.put("group.id", config.getString("group.id"));
        props.put("zookeeper.session.timeout.ms", config.getString("zookeeper.session.timeout.ms"));
        props.put("zookeeper.sync.time.ms", config.getString("zookeeper.sync.time.ms"));
        props.put("auto.commit.interval.ms", config.getString("auto.commit.interval.ms"));
        ConsumerConfig consumerConfig = new ConsumerConfig(props);

        // connector
        this.connector = Consumer.createJavaConsumerConnector(consumerConfig);

        // connect to kafka
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(TOPIC, config.getInt("processor.threads"));
        Map<String, List<KafkaStream<String, CommunicatorMessage>>> consumerMap = this.connector.createMessageStreams(topicCountMap, new StringDecoder(null), new CommunicatorMessageDecoder());
        List<KafkaStream<String, CommunicatorMessage>> streams = consumerMap.get(TOPIC);

        // executor
        this.executor = Executors.newFixedThreadPool(config.getInt("processor.threads"));

        // launch all workers
        for (final KafkaStream<String, CommunicatorMessage> stream : streams) {
            executor.submit(new ProcessorWorker(stream, factory.newListener()));
        }
    }

    @Override
    public void destroy() {
        if (connector != null) connector.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }
}
