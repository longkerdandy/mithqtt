package com.github.longkerdandy.mithril.mqtt.storage.redis;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.api.internal.Publish;
import com.lambdaworks.redis.ValueScanCursor;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.List;
import java.util.Map;

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
     * Get next packet id for the client
     *
     * @param clientId Client Id
     * @return Next Packet Id
     */
    int getNextPacketId(String clientId);

    /**
     * Get specific in-flight message for the client
     *
     * @param clientId Client Id
     * @param packetId Packet Id
     * @return In-Flight Message
     */
    InternalMessage getInFlightMessage(String clientId, int packetId);

    /**
     * Add in-flight message for the client
     *
     * @param clientId Client Id
     * @param packetId Packet Id
     * @param msg      In-Flight Message
     * @param dup      Duplicated
     */
    void addInFlightMessage(String clientId, int packetId, InternalMessage msg, boolean dup);

    /**
     * Remove specific in-flight message for the client
     *
     * @param clientId Client Id
     * @param packetId Packet Id
     */
    void removeInFlightMessage(String clientId, int packetId);

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

    /**
     * Remove all in-flight message for the client
     *
     * @param clientId Client Id
     */
    void removeAllInFlightMessage(String clientId);

    /**
     * Add unacknowledged qos 2 PUBLISH message's packet id from the client
     *
     * @param clientId Client Id
     * @param packetId Packet Id
     * @return Packet Id existed?
     */
    boolean addQoS2MessageId(String clientId, int packetId);

    /**
     * Remove unacknowledged qos 2 PUBLISH message's packet id from the client
     *
     * @param clientId Client Id
     * @param packetId Packet Id
     * @return Packet Id existed?
     */
    boolean removeQoS2MessageId(String clientId, int packetId);

    /**
     * Remove all unacknowledged qos 2 PUBLISH message's packet id from the client
     *
     * @param clientId Client Id
     */
    void removeAllQoS2MessageId(String clientId);

    /**
     * Update topic subscription for the client
     * Topic Levels must be sanitized
     *
     * @param clientId    Client Id
     * @param topicLevels List of topic levels
     * @param qos         Subscription QoS
     */
    void updateSubscription(String clientId, List<String> topicLevels, MqttQoS qos);

    /***
     * Remove topic name subscription for the client
     * Topic Levels must be sanitized
     *
     * @param clientId    Client Id
     * @param topicLevels List of topic levels
     */
    void removeSubscription(String clientId, List<String> topicLevels);

    /**
     * Remove all subscriptions for the client
     *
     * @param clientId Client Id
     */
    void removeAllSubscriptions(String clientId);

    /**
     * Get all subscriptions matching the topic
     * This is a recursion method
     *
     * @param topicLevels List of topic levels
     * @param map         Return Value: Subscriptions Key - Client Id, Value - QoS
     */
    void getMatchSubscriptions(List<String> topicLevels, Map<String, MqttQoS> map);

    /**
     * Get specific retain message for the topic name
     *
     * @param topicLevels Topic Levels
     * @param packetId    Packet Id
     * @return Retain message in Map format
     */
    InternalMessage<Publish> getRetainMessage(List<String> topicLevels, int packetId);

    /**
     * Add retain message for the topic name
     *
     * @param topicLevels Topic Levels
     * @param packetId    Packet Id
     * @param msg         Retain Message
     */
    void addRetainMessage(List<String> topicLevels, int packetId, InternalMessage<Publish> msg);

    /**
     * Get all retain messages the topic name
     *
     * @param topicLevels Topic Levels
     * @return List of Retain Message
     */
    List<InternalMessage<Publish>> getAllRetainMessages(List<String> topicLevels);

    /**
     * Remove all retain messages for the topic name
     *
     * @param topicLevels Topic Levels
     */
    void removeAllRetainMessage(List<String> topicLevels);
}
