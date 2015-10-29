package com.github.longkerdandy.mithril.mqtt.processor;

import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorListenerFactory;
import com.github.longkerdandy.mithril.mqtt.processor.comm.ProcessorListenerFactoryImpl;
import com.github.longkerdandy.mithril.mqtt.storage.redis.sync.RedisSyncStorage;
import com.lambdaworks.redis.RedisURI;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MQTT Processor
 */
public class MqttProcessor {

    private static final Logger logger = LoggerFactory.getLogger(MqttProcessor.class);

    public static void main(String[] args) throws Exception {

        logger.debug("Starting MQTT processor ...");

        // load config
        logger.debug("Loading MQTT processor config files ...");
        // PropertiesConfiguration processorConfig;
        PropertiesConfiguration redisConfig;
        PropertiesConfiguration communicatorConfig;
        if (args.length >= 3) {
            // processorConfig = new PropertiesConfiguration(args[0]);
            redisConfig = new PropertiesConfiguration(args[1]);
            communicatorConfig = new PropertiesConfiguration(args[2]);
        } else {
            // processorConfig = new PropertiesConfiguration("config/processor.properties");
            redisConfig = new PropertiesConfiguration("config/redis.properties");
            communicatorConfig = new PropertiesConfiguration("config/communicator.properties");
        }

        // storage
        logger.debug("Initializing redis storage ...");
        RedisSyncStorage redis = (RedisSyncStorage) Class.forName(redisConfig.getString("storage.sync.class")).newInstance();
        redis.init(RedisURI.create(redisConfig.getString("redis.uri")));

        // communicator
        logger.debug("Initializing communicator ...");
        ProcessorCommunicator communicator = (ProcessorCommunicator) Class.forName(communicatorConfig.getString("communicator.class")).newInstance();
        ProcessorListenerFactory listenerFactory = new ProcessorListenerFactoryImpl(communicator, redis);
        communicator.init(communicatorConfig, listenerFactory);

        logger.info("MQTT processor is up and running.");
    }
}
