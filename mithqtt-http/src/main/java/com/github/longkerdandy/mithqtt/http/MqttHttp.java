package com.github.longkerdandy.mithqtt.http;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.longkerdandy.mithqtt.http.oauth.OAuthAuthenticator;
import com.github.longkerdandy.mithqtt.http.resources.MqttSubscribeResource;
import com.github.longkerdandy.mithqtt.http.resources.MqttUnsubscribeResource;
import com.github.longkerdandy.mithqtt.http.util.Validator;
import com.github.longkerdandy.mithqtt.storage.redis.sync.RedisSyncStorage;
import com.github.longkerdandy.mithqtt.api.auth.Authenticator;
import com.github.longkerdandy.mithqtt.api.comm.HttpCommunicator;
import com.github.longkerdandy.mithqtt.api.metrics.MetricsService;
import com.github.longkerdandy.mithqtt.http.resources.MqttPublishResource;
import com.sun.security.auth.UserPrincipal;
import io.dropwizard.Application;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.auth.PermitAllAuthorizer;
import io.dropwizard.auth.oauth.OAuthCredentialAuthFilter;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.ArrayUtils;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
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
            redisConfig = new PropertiesConfiguration(args[0]);
            communicatorConfig = new PropertiesConfiguration(args[1]);
            authenticatorConfig = new PropertiesConfiguration(args[2]);
            metricsConfig = new PropertiesConfiguration(args[3]);

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
        // validator
        Validator validator = new Validator(configuration);

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
        final MetricsService metrics = metricsEnabled ? (MetricsService) Class.forName(metricsConfig.getString("metrics.class")).newInstance() : null;
        if (metricsEnabled) {
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

        // OAuth
        environment.jersey().register(new AuthDynamicFeature(
                new OAuthCredentialAuthFilter.Builder<UserPrincipal>()
                        .setAuthenticator(new OAuthAuthenticator(authenticator))
                        .setAuthorizer(new PermitAllAuthorizer<>())
                        .setPrefix("Bearer")
                        .buildAuthFilter()));
        environment.jersey().register(RolesAllowedDynamicFeature.class);
        environment.jersey().register(new AuthValueFactoryProvider.Binder<>(UserPrincipal.class));

        // register resources
        environment.jersey().register(new MqttPublishResource(configuration.getServerId(), validator, redis, communicator, authenticator, metrics));
        environment.jersey().register(new MqttSubscribeResource(configuration.getServerId(), validator, redis, communicator, authenticator, metrics));
        environment.jersey().register(new MqttUnsubscribeResource(configuration.getServerId(), validator, redis, communicator, authenticator, metrics));

        // config jackson
        environment.getObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        environment.getObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        environment.getObjectMapper().configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        environment.getObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }
}
