package com.github.longkerdandy.mithril.mqtt.storage.redis.async;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.RedisURI;

/**
 * Redis Asynchronous Storage
 */
@SuppressWarnings("unused")
public interface RedisAsyncStorage {

    /**
     * Init the storage
     * Should be could before using redis storage
     *
     * @param redisURI Redis URI
     */
    void init(RedisURI redisURI);

    /**
     * Destroy the storage
     * Should be could when gracefully shutdown
     */
    void destroy();

    /**
     * Get session existence for the client
     *
     * @param clientId Client Id
     * @return Session Existence (1 clean session, 0 normal session, null not exist)
     */
    RedisFuture<String> getSessionExist(String clientId);

    /**
     * Remove unacknowledged qos 2 PUBLISH message's packet id from the client
     *
     * @param clientId Client Id
     * @param packetId Packet Id
     * @return 1 packet id removed, 0 packet id not exist
     */
    RedisFuture<Long> removeQoS2MessageId(String clientId, int packetId);

    /**
     * Add in-flight message for the client
     *
     * @param clientId Client Id
     * @param packetId Packet Id
     * @param msg      In-Flight Message
     * @param dup      Duplicated
     * @return RedisFuture
     */
    RedisFuture<String> addInFlightMessage(String clientId, int packetId, InternalMessage msg, boolean dup);

    /**
     * Remove specific in-flight message for the client
     *
     * @param clientId Client Id
     * @param packetId Packet Id
     * @return RedisFuture
     */
    RedisFuture<String> removeInFlightMessage(String clientId, int packetId);
}
