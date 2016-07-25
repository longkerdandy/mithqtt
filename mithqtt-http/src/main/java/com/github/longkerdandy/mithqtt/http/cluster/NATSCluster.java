package com.github.longkerdandy.mithqtt.http.cluster;

import com.github.longkerdandy.mithqtt.api.message.Message;
import com.github.longkerdandy.mithqtt.util.JSONs;
import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static io.nats.client.ConnectionFactory.*;

/**
 * NATS used as cluster message implementation
 */
public class NATSCluster {

    private static final Logger logger = LoggerFactory.getLogger(NATSCluster.class);

    // topics
    private String BROKER_TOPIC_PREFIX;
    private String APPLICATION_TOPIC;

    // active connection to NATS
    private Connection conn;

    /**
     * Initialize
     *
     * @param config Cluster configuration
     * @throws IOException      if the connection with NATS cannot be established for some reason.
     * @throws TimeoutException if the connection with NATS timeout has been exceeded.
     */
    public void init(AbstractConfiguration config) throws IOException, TimeoutException {
        logger.trace("Loading cluster configurations ...");

        // Topics for broker and application
        BROKER_TOPIC_PREFIX = config.getString("broker.topic");
        APPLICATION_TOPIC = config.getString("application.topic");

        // Setup options to include all servers in the cluster
        ConnectionFactory cf = new ConnectionFactory();
        cf.setServers(config.getString("nats.servers", DEFAULT_URL).split(","));

        // Set ReconnectWait and MaxReconnect attempts.
        cf.setMaxReconnect(config.getInt("nats.maxReconnect", DEFAULT_MAX_RECONNECT));
        cf.setReconnectWait(config.getInt("nats.reconnectWait", DEFAULT_RECONNECT_WAIT));

        // Optionally disable randomization of the server pool
        cf.setNoRandomize(config.getBoolean("nats.noRandomize", false));

        logger.trace("Creating connection with NATS servers ...");

        // Create connection to the NATS servers
        this.conn = cf.createConnection();
    }

    /**
     * Destroy
     */
    public void destroy() {
        logger.trace("Closing connection with NATS servers ...");

        if (this.conn != null) this.conn.close();
    }

    /**
     * Send message to specific broker
     *
     * @param brokerId Broker Id which will receive the message
     * @param message  Mqtt Message (internal format)
     */
    public void sendToBroker(String brokerId, Message message) {
        String brokerTopic = BROKER_TOPIC_PREFIX + "." + brokerId;
        try {
            this.conn.publish(brokerTopic, JSONs.Mapper.writeValueAsBytes(message));
        } catch (IOException e) {
            logger.warn("Cluster Error: Failed to send message {} to topic {}: ", message.fixedHeader().messageType(), brokerTopic, e);
        }
    }

    /**
     * Send message to 3rd party application
     *
     * @param message Mqtt Message (internal format)
     */
    public void sendToApplication(Message message) {
        try {
            this.conn.publish(APPLICATION_TOPIC, JSONs.Mapper.writeValueAsBytes(message));
        } catch (IOException e) {
            logger.warn("Cluster Error: Failed to send message {} to topic {}: ", message.fixedHeader().messageType(), APPLICATION_TOPIC, e);
        }
    }
}
