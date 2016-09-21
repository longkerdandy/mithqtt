package com.github.longkerdandy.mithqtt.http;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.longkerdandy.mithqtt.api.auth.Authenticator;
import com.github.longkerdandy.mithqtt.api.cluster.Cluster;
import com.github.longkerdandy.mithqtt.http.oauth.OAuthAuthenticator;
import com.github.longkerdandy.mithqtt.http.resources.MqttPublishResource;
import com.github.longkerdandy.mithqtt.http.resources.MqttSubscribeResource;
import com.github.longkerdandy.mithqtt.http.resources.MqttUnsubscribeResource;
import com.github.longkerdandy.mithqtt.http.util.Validator;
import com.github.longkerdandy.mithqtt.storage.sync.SyncStorage;
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

    private static PropertiesConfiguration storageConfig;
    private static PropertiesConfiguration clusterConfig;
    private static PropertiesConfiguration authenticatorConfig;

    public static void main(String[] args) throws Exception {
        logger.debug("Starting MQTT http ...");

        // load config
        logger.debug("Loading MQTT http config files ...");
        if (args.length >= 3) {
            storageConfig = new PropertiesConfiguration(args[0]);
            clusterConfig = new PropertiesConfiguration(args[1]);
            authenticatorConfig = new PropertiesConfiguration(args[2]);

            if (args.length >= 4) {
                args = ArrayUtils.subarray(args, 4, args.length);
            } else {
                args = new String[]{};
            }
        } else {
            storageConfig = new PropertiesConfiguration("config/storage.properties");
            clusterConfig = new PropertiesConfiguration("config/cluster.properties");
            authenticatorConfig = new PropertiesConfiguration("config/authenticator.properties");
        }

        new MqttHttp().run(args);
    }

    @Override
    public void run(MqttHttpConfiguration configuration, Environment environment) throws Exception {
        // validator
        logger.debug("Initializing validator ...");
        Validator validator = new Validator(configuration);

        // storage
        SyncStorage storage = (SyncStorage) Class.forName(storageConfig.getString("storage.sync.class")).newInstance();
        environment.lifecycle().manage(new Managed() {
            @Override
            public void start() throws Exception {
                logger.debug("Initializing storage storage ...");
                storage.init(storageConfig);
            }

            @Override
            public void stop() throws Exception {
                logger.debug("Destroying storage storage ...");
                storage.destroy();
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

        // cluster
        Cluster cluster = (Cluster) Class.forName(clusterConfig.getString("cluster.class")).newInstance();
        environment.lifecycle().manage(new Managed() {
            @Override
            public void start() throws Exception {
                logger.debug("Initializing cluster ...");
                cluster.init(clusterConfig, null);
            }

            @Override
            public void stop() throws Exception {
                logger.debug("Destroying cluster ...");
                cluster.destroy();
            }
        });

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
        environment.jersey().register(new MqttPublishResource(configuration.getServerId(), validator, storage, cluster, authenticator));
        environment.jersey().register(new MqttSubscribeResource(configuration.getServerId(), validator, storage, cluster, authenticator));
        environment.jersey().register(new MqttUnsubscribeResource(configuration.getServerId(), validator, storage, cluster, authenticator));

        // config jackson
        environment.getObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        environment.getObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        environment.getObjectMapper().configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        environment.getObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }
}
