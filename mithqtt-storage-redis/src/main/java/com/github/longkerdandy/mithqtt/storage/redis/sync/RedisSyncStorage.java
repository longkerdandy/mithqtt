package com.github.longkerdandy.mithqtt.storage.redis.sync;

import com.github.longkerdandy.mithqtt.api.message.Message;
import com.github.longkerdandy.mithqtt.api.message.MqttPublishPayload;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.configuration.AbstractConfiguration;

import java.util.List;
import java.util.Map;

/**
 * Redis Synchronized Storage
 */
@SuppressWarnings("unused")
public interface RedisSyncStorage {

    /**
     * Init the storage
     * Should be invoked before using redis storage
     *
     * @param config Redis Configuration
     */
    void init(AbstractConfiguration config);

    /**
     * Destroy the storage
     * Should be invoked when gracefully shutdown
     */
    void destroy();

    /**
     * Try to lock specific client to specific state
     * State can only be CONNECTING & DISCONNECTING
     *
     * @param clientId Client Id
     * @param state    Client State
     * @return True if current state is CONNECTED or DISCONNECTED
     */
    boolean lock(String clientId, int state);

    /**
     * Try to release specific client to specific state
     * State can only be CONNECTED & DISCONNECTED
     *
     * @param clientId Client Id
     * @param state    Client State
     * @return True if current state is CONNECTING or DISCONNECTING
     */
    boolean release(String clientId, int state);

    /**
     * Get connected mqtt broker node (id) for the client
     *
     * @param clientId Client Id
     * @return MQTT Broker Node (Id)
     */
    String getConnectedNode(String clientId);

    /**
     * Update connected mqtt broker node (id) for the client
     *
     * @param clientId Client Id
     * @param node     MQTT Broker Node (Id)
     * @param seconds  TTL
     * @return Previous connected MQTT Broker Node (Id), Null if not exist
     */
    String updateConnectedNode(String clientId, String node, int seconds);

    /**
     * Remove connected mqtt broker node (id) for the client
     *
     * @param clientId Client Id
     * @param node     MQTT Broker Node (Id)
     * @return Connected node removed? (Exist)
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
     * @return Session removed? (Exist)
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
    Message getInFlightMessage(String clientId, int packetId);

    /**
     * Add in-flight message for the client
     *
     * @param clientId Client Id
     * @param packetId Packet Id
     * @param msg      In-Flight Message
     * @param dup      Duplicated
     */
    void addInFlightMessage(String clientId, int packetId, Message msg, boolean dup);

    /**
     * Add in-flight message for the client but expires in certain duration
     *
     * @param clientId Client Id
     * @param packetId Packet Id
     * @param msg      In-Flight Message
     * @param dup      Duplicated
     * @param ttl      Time To Live in seconds
     */
    void addInFlightMessage(String clientId, int packetId, Message msg, boolean dup, long ttl);

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
     * @return List of In-Flight Message
     */
    List<Message> getAllInFlightMessages(String clientId);

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
     * @return Packet Id added? (Not exist)
     */
    boolean addQoS2MessageId(String clientId, int packetId);

    /**
     * Remove unacknowledged qos 2 PUBLISH message's packet id from the client
     *
     * @param clientId Client Id
     * @param packetId Packet Id
     * @return Packet Id removed? (Exist)
     */
    boolean removeQoS2MessageId(String clientId, int packetId);

    /**
     * Remove all unacknowledged qos 2 PUBLISH message's packet id from the client
     *
     * @param clientId Client Id
     */
    void removeAllQoS2MessageId(String clientId);

    /**
     * Get the topic's subscriptions
     * Topic Levels must been sanitized
     *
     * @param topicLevels List of topic levels
     * @return Subscriptions: Key - Client Id, Value - QoS
     */
    Map<String, MqttQoS> getTopicSubscriptions(List<String> topicLevels);

    /**
     * Get the client's subscriptions
     *
     * @param clientId Client Id
     * @return Subscriptions: Key - Topic, Value - QoS
     */
    Map<String, MqttQoS> getClientSubscriptions(String clientId);

    /**
     * Update topic subscription for the client
     * Topic Levels must been sanitized
     *
     * @param clientId    Client Id
     * @param topicLevels List of topic levels
     * @param qos         Subscription QoS
     */
    void updateSubscription(String clientId, List<String> topicLevels, MqttQoS qos);

    /***
     * Remove topic name subscription for the client
     * Topic Levels must been sanitized
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
     * @param map         RETURN VALUE! Subscriptions: Key - Client Id, Value - QoS
     */
    void getMatchSubscriptions(List<String> topicLevels, Map<String, MqttQoS> map);

    /**
     * Add retain message for the topic name
     * Retain id will be generated
     *
     * @param topicLevels Topic Levels
     * @param msg         Retain Message
     * @return Retain Id
     */
    int addRetainMessage(List<String> topicLevels, Message<MqttPublishVariableHeader, MqttPublishPayload> msg);

    /**
     * Remove all retain messages for the topic name
     *
     * @param topicLevels Topic Levels
     */
    void removeAllRetainMessage(List<String> topicLevels);

    /**
     * Get all retain messages the topic name
     *
     * @param topicLevels Topic Levels
     * @return List of Retain Message
     */
    List<Message<MqttPublishVariableHeader, MqttPublishPayload>> getMatchRetainMessages(List<String> topicLevels);
}
