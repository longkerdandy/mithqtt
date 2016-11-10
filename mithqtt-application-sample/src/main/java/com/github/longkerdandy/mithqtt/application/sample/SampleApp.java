package com.github.longkerdandy.mithqtt.application.sample;

import com.github.longkerdandy.mithqtt.api.cluster.Cluster;
import com.github.longkerdandy.mithqtt.application.sample.cluster.ApplicationClusterListenerFactoryImpl;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample Application
 */
public class SampleApp {

    private static final Logger logger = LoggerFactory.getLogger(SampleApp.class);

    public static void main(String[] args) throws Exception {

        logger.debug("Starting Sample Application ...");

        // load config
        logger.debug("Loading Sample Application config files ...");
        PropertiesConfiguration clusterConfig;
        if (args.length >= 1 ) {
            clusterConfig = new PropertiesConfiguration(args[0]);
        } else {
            clusterConfig = new PropertiesConfiguration("config/cluster.properties");
        }

        // cluster
        logger.debug("Initializing cluster ...");
        Cluster cluster = (Cluster) Class.forName(clusterConfig.getString("cluster.class")).newInstance();
        cluster.init(clusterConfig, new ApplicationClusterListenerFactoryImpl());
    }
}
