package com.github.longkerdandy.mithril.mqtt.http;

import com.github.longkerdandy.mithril.mqtt.api.auth.Authenticator;
import com.github.longkerdandy.mithril.mqtt.api.comm.HttpCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.metrics.MetricsService;
import com.github.longkerdandy.mithril.mqtt.storage.redis.sync.RedisSyncStorage;
import io.dropwizard.Application;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MQTT Http Interface
 */
public class MqttHttp extends Application<MqttHttpConfiguration> {

    private static final Logger logger = LoggerFactory.getLogger(MqttHttp.class);

    private static PropertiesConfiguration redisConfig;
    private static PropertiesConfiguration communicatorConfig;
    private static PropertiesConfiguration authenticatorConfig;
    private static PropertiesConfiguration metricsConfig;

    public static void main(String[] args) throws Exception {
        if (args.length >= 4) {
            redisConfig = new PropertiesConfiguration(args[1]);
            communicatorConfig = new PropertiesConfiguration(args[2]);
            authenticatorConfig = new PropertiesConfiguration(args[3]);
            metricsConfig = new PropertiesConfiguration(args[4]);

            if (args.length >= 5) {
                args = ArrayUtils.subarray(args, 4, args.length);
            } else {
                args = new String[]{};
            }
        } else {
            redisConfig = new PropertiesConfiguration("config/redis.properties");
            communicatorConfig = new PropertiesConfiguration("config/communicator.properties");
            authenticatorConfig = new PropertiesConfiguration("config/authenticator.properties");
            metricsConfig = new PropertiesConfiguration("config/metrics.properties");
        }

        new MqttHttp().run(args);
    }

    @Override
    public void run(MqttHttpConfiguration configuration, Environment environment) throws Exception {
        // storage
        RedisSyncStorage redis = (RedisSyncStorage) Class.forName(redisConfig.getString("storage.sync.class")).newInstance();
        environment.lifecycle().manage(new Managed() {
            @Override
            public void start() throws Exception {
                logger.debug("Initializing redis storage ...");
                redis.init(redisConfig);
            }

            @Override
            public void stop() throws Exception {
                logger.debug("Destroying redis storage ...");
                redis.destroy();
            }
        });

        // communicator
        HttpCommunicator communicator = (HttpCommunicator) Class.forName(communicatorConfig.getString("communicator.class")).newInstance();
        environment.lifecycle().manage(new Managed() {
            @Override
            public void start() throws Exception {
                logger.debug("Initializing communicator ...");
                communicator.init(communicatorConfig, configuration.getServerId());
            }

            @Override
            public void stop() throws Exception {
                logger.debug("Destroying communicator ...");
                communicator.destroy();
            }
        });

        // authenticator
        Authenticator authenticator = (Authenticator) Class.forName(authenticatorConfig.getString("authenticator.class")).newInstance();
        environment.lifecycle().manage(new Managed() {
            @Override
            public void start() throws Exception {
                logger.debug("Initializing authenticator ...");
                authenticator.init(authenticatorConfig);
            }

            @Override
            public void stop() throws Exception {
                logger.debug("Destroying authenticator ...");
                authenticator.destroy();
            }
        });

        // metrics
        final boolean metricsEnabled = metricsConfig.getBoolean("metrics.enabled");
        if (metricsEnabled) {
            MetricsService metrics = (MetricsService) Class.forName(metricsConfig.getString("metrics.class")).newInstance();
            environment.lifecycle().manage(new Managed() {
                @Override
                public void start() throws Exception {
                    logger.debug("Initializing metrics ...");
                    metrics.init(metricsConfig);
                }

                @Override
                public void stop() throws Exception {
                    logger.debug("Destroying metrics ...");
                    metrics.destroy();
                }
            });
        }
    }
}
