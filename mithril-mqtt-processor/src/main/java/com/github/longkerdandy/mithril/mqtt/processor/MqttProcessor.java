package com.github.longkerdandy.mithril.mqtt.processor;

import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorListenerFactory;
import com.github.longkerdandy.mithril.mqtt.processor.comm.ProcessorListenerFactoryImpl;
import com.github.longkerdandy.mithril.mqtt.storage.redis.RedisSyncStorage;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * MQTT Processor
 */
public class MqttProcessor {

    public static void main(String[] args) throws Exception {
        // load config
        PropertiesConfiguration processorConfig = args.length >= 2 ?
                new PropertiesConfiguration(args[0]) : new PropertiesConfiguration("config/processor.properties");
        PropertiesConfiguration communicatorConfig = args.length >= 2 ?
                new PropertiesConfiguration(args[0]) : new PropertiesConfiguration("config/communicator.properties");

        // storage
        RedisSyncStorage redis = null;

        // communicator
        ProcessorCommunicator communicator = (ProcessorCommunicator) Class.forName(processorConfig.getString("communicator.class")).newInstance();
        ProcessorListenerFactory listenerFactory = new ProcessorListenerFactoryImpl(communicator, redis);
        communicator.init(communicatorConfig, listenerFactory);
    }
}
