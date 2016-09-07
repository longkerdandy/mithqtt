package com.github.longkerdandy.mithqtt.cluster.nats;

import com.github.longkerdandy.mithqtt.api.cluster.Cluster;
import com.github.longkerdandy.mithqtt.api.cluster.ClusterException;
import com.github.longkerdandy.mithqtt.api.cluster.ClusterListener;
import com.github.longkerdandy.mithqtt.api.cluster.ClusterListenerFactory;
import com.github.longkerdandy.mithqtt.api.message.Message;
import com.github.longkerdandy.mithqtt.util.JSONs;
import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static io.nats.client.ConnectionFactory.*;

/**
 * NATS Server based Cluster
 */
public class NATSClusterImpl implements Cluster {

    private static final Logger logger = LoggerFactory.getLogger(NATSClusterImpl.class);

    // topics
    private String LISTENER_TOPIC;
    private String BROKER_TOPIC_PREFIX;
    private String APPLICATION_TOPIC;

    // active connection to NATS Server
    private Connection conn;

    @SuppressWarnings("unchecked")
    @Override
    public void init(AbstractConfiguration config, ClusterListenerFactory factory) throws ClusterException {
        logger.trace("Loading cluster configurations ...");

        // Topics for broker and application
        LISTENER_TOPIC = config.getString("listener.topic");
        BROKER_TOPIC_PREFIX = config.getString("broker.topic.prefix");
        APPLICATION_TOPIC = config.getString("application.topic");

        // Setup options to include all servers in the cluster
        ConnectionFactory cf = new ConnectionFactory();
        cf.setServers(config.getString("nats.servers", DEFAULT_URL).split(","));

        // Set NATS configurations
        cf.setMaxReconnect(config.getInt("nats.maxReconnect", DEFAULT_MAX_RECONNECT));
        cf.setReconnectWait(config.getInt("nats.reconnectWait", DEFAULT_RECONNECT_WAIT));
        cf.setReconnectBufSize(config.getInt("nats.reconnectBufSize", DEFAULT_RECONNECT_BUF_SIZE));
        cf.setConnectionTimeout(config.getInt("nats.connectionTimeout", DEFAULT_TIMEOUT));
        cf.setPingInterval(config.getInt("nats.pingInterval", DEFAULT_PING_INTERVAL));
        cf.setMaxPingsOut(config.getInt("nats.maxPingsOut", DEFAULT_MAX_PINGS_OUT));
        cf.setMaxPendingMsgs(config.getInt("nats.maxPendingMsgs", DEFAULT_MAX_PENDING_MSGS));
        cf.setMaxPendingBytes(config.getInt("nats.maxPendingBytes", DEFAULT_MAX_PENDING_BYTES));

        // Optionally disable randomization of the server pool
        cf.setNoRandomize(config.getBoolean("nats.noRandomize", false));

        logger.trace("Creating connection with NATS servers ...");

        // Create connection to the NATS servers
        try {
            this.conn = cf.createConnection();
        } catch (IOException | TimeoutException e) {
            throw new ClusterException(e);
        }

        if (StringUtils.isNotBlank(LISTENER_TOPIC) && factory != null) {
            logger.trace("Subscribe to topic {} ...", LISTENER_TOPIC);

            this.conn.subscribeAsync(LISTENER_TOPIC, msg -> {
                try {
                    logger.trace("Received message from NATS topic {}", msg.getSubject());

                    // event listener
                    ClusterListener listener = factory.newListener();

                    // decode message
                    Message m = JSONs.decodeMessage(msg.getData());

                    // handle message
                    if (m != null) {
                        logger.debug("Cluster received: Received {} message for client {}", m.fixedHeader().messageType(), m.additionalHeader().clientId());
                        switch (m.fixedHeader().messageType()) {
                            case CONNECT:
                                listener.onConnect(m);
                                break;
                            case SUBSCRIBE:
                                listener.onSubscribe(m);
                                break;
                            case UNSUBSCRIBE:
                                listener.onUnsubscribe(m);
                                break;
                            case PUBLISH:
                                listener.onPublish(m);
                                break;
                            case DISCONNECT:
                                listener.onDisconnect(m);
                                break;
                            default:
                                logger.warn("Cluster Error: Received message with unknown type {}", m.fixedHeader().messageType());
                        }
                    }
                } catch (IOException e) {
                    logger.warn("Cluster Error: Error when decoding or handling the message", e);
                }
            });
        }
    }

    @Override
    public void destroy() {
        logger.trace("Closing connection with NATS servers ...");

        if (this.conn != null) this.conn.close();
    }

    @Override
    public void sendToBroker(String brokerId, Message message) {
        String brokerTopic = BROKER_TOPIC_PREFIX + "." + brokerId;
        try {
            this.conn.publish(brokerTopic, JSONs.Mapper.writeValueAsBytes(message));
        } catch (IOException e) {
            logger.warn("Cluster Error: Failed to send message {} to topic {}: ", message.fixedHeader().messageType(), brokerTopic, e);
        }
    }

    @Override
    public void sendToApplication(Message message) {
        try {
            this.conn.publish(APPLICATION_TOPIC, JSONs.Mapper.writeValueAsBytes(message));
        } catch (IOException e) {
            logger.warn("Cluster Error: Failed to send message {} to topic {}: ", message.fixedHeader().messageType(), APPLICATION_TOPIC, e);
        }
    }
}
