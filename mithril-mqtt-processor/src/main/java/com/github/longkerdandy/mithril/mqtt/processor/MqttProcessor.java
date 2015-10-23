package com.github.longkerdandy.mithril.mqtt.processor;

import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorListenerFactory;
import com.github.longkerdandy.mithril.mqtt.processor.comm.ProcessorListenerFactoryImpl;
import com.github.longkerdandy.mithril.mqtt.storage.redis.sync.RedisSyncStorage;
import com.lambdaworks.redis.RedisURI;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * MQTT Processor
 */
public class MqttProcessor {

    public static void main(String[] args) throws Exception {
        // load config
        PropertiesConfiguration processorConfig;
        PropertiesConfiguration redisConfig;
        PropertiesConfiguration communicatorConfig;
        if (args.length >= 3) {
            processorConfig = new PropertiesConfiguration(args[0]);
            redisConfig = new PropertiesConfiguration(args[1]);
            communicatorConfig = new PropertiesConfiguration(args[2]);
        } else {
            processorConfig = new PropertiesConfiguration("config/broker.properties");
            redisConfig = new PropertiesConfiguration("config/redis.properties");
            communicatorConfig = new PropertiesConfiguration("config/communicator.properties");
        }

        // storage
        RedisSyncStorage redis = (RedisSyncStorage) Class.forName(redisConfig.getString("storage.sync.class")).newInstance();
        redis.init(RedisURI.create(redisConfig.getString("redis.uri")));

        // communicator
        ProcessorCommunicator communicator = (ProcessorCommunicator) Class.forName(communicatorConfig.getString("communicator.class")).newInstance();
        ProcessorListenerFactory listenerFactory = new ProcessorListenerFactoryImpl(communicator, redis);
        communicator.init(communicatorConfig, listenerFactory);
    }
}
