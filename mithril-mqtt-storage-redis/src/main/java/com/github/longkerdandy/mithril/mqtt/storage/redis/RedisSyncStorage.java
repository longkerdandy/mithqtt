package com.github.longkerdandy.mithril.mqtt.storage.redis;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.lambdaworks.redis.ValueScanCursor;

import java.util.List;

/**
 * Redis Synchronized Storage
 */
public interface RedisSyncStorage {

    /**
     * Iteration connected clients for the mqtt server node
     *
     * @param node   MQTT Broker Node
     * @param cursor Scan Cursor
     * @param count  Limit
     * @return Clients and Cursor
     */
    ValueScanCursor<String> getConnectedClients(String node, String cursor, long count);

    /**
     * Get connected mqtt broker node for the client
     *
     * @param clientId Client Id
     * @return MQTT Broker Node
     */
    String getConnectedNode(String clientId);

    /**
     * Update connected mqtt broker node for the client
     *
     * @param clientId Client Id
     * @param node     MQTT Broker Node
     * @return Previous connected MQTT Broker Node
     */
    String updateConnectedNode(String clientId, String node);

    /**
     * Remove connected mqtt server node for the client
     *
     * @param clientId Client Id
     * @param node     MQTT Server Node
     * @return Existed and removed?
     */
    boolean removeConnectedNode(String clientId, String node);

    /**
     * Get session existence for the client
     *
     * @param clientId Client Id
     * @return Session Existence (1 clean session, 0 normal session, < 0 not exist)
     */
    int getSessionExist(String clientId);

    /**
     * Update session existence for the client
     *
     * @param clientId     Client Id
     * @param cleanSession Clean Session
     */
    void updateSessionExist(String clientId, boolean cleanSession);

    /**
     * Remove session existence for the client
     *
     * @param clientId Client Id
     * @return Removed?
     */
    boolean removeSessionExist(String clientId);

    /**
     * Remove all session state
     *
     * @param clientId Client Id
     */
    void removeAllSessionState(String clientId);

    /**
     * Get all in-flight message's for the client
     * Including:
     * QoS 1 and QoS 2 PUBLISH messages which have been sent to the Client, but have not been acknowledged.
     * QoS 0, QoS 1 and QoS 2 PUBLISH messages pending transmission to the Client.
     * QoS 2 PUBREL messages which have been sent from the Client, but have not been acknowledged.
     *
     * @param clientId Client Id
     * @return List of Internal Message
     */
    List<InternalMessage> getAllInFlightMessages(String clientId);
}
