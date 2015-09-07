package com.github.longkerdandy.mithril.mqtt.storage.redis;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.ScriptOutputType;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import com.lambdaworks.redis.api.async.RedisHashAsyncCommands;
import com.lambdaworks.redis.api.async.RedisListAsyncCommands;
import com.lambdaworks.redis.api.async.RedisSetAsyncCommands;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;
import org.apache.commons.lang3.BooleanUtils;

import java.io.UnsupportedEncodingException;
import java.util.*;

import static io.netty.buffer.Unpooled.wrappedBuffer;

/**
 * Redis Storage
 */
public class RedisStorage {

    // A scalable thread-safe Redis client. Multiple threads may share one connection if they avoid
    // blocking and transactional operations such as BLPOP and MULTI/EXEC.
    protected RedisClient client;
    // A thread-safe connection to a redis server. Multiple threads may share one StatefulRedisConnection
    protected StatefulRedisConnection<String, String> conn;

    public RedisStorage(String host, int port) {
        this.client = new RedisClient(host, port);
    }

    /**
     * Convert Map to MqttMessage
     *
     * @param map Map
     * @return MqttMessage
     */
    public static MqttMessage mapToMqtt(Map<String, String> map) {
        int type = Integer.parseInt(map.get("type"));
        if (type == MqttMessageType.PUBLISH.value()) {
            byte[] payload = null;
            if (map.get("payload") != null) try {
                payload = map.get("payload").getBytes("ISO-8859-1");
            } catch (UnsupportedEncodingException ignore) {
            }
            return MqttMessageFactory.newMessage(
                    new MqttFixedHeader(
                            MqttMessageType.PUBLISH,
                            BooleanUtils.toBoolean(map.getOrDefault("dup", "0"), "1", "0"),
                            MqttQoS.valueOf(Integer.parseInt(map.getOrDefault("qos", "0"))),
                            BooleanUtils.toBoolean(map.getOrDefault("retain", "0"), "1", "0"),
                            0
                    ),
                    new MqttPublishVariableHeader(map.get("topicName"), Integer.parseInt(map.getOrDefault("packetId", "0"))),
                    payload == null ? null : wrappedBuffer(payload)
            );
        } else if (type == MqttMessageType.PUBREL.value()) {
            return MqttMessageFactory.newMessage(
                    new MqttFixedHeader(
                            MqttMessageType.PUBREL,
                            false,
                            MqttQoS.AT_LEAST_ONCE,
                            false,
                            0
                    ),
                    MqttMessageIdVariableHeader.from(Integer.parseInt(map.getOrDefault("packetId", "0"))),
                    null
            );
        } else {
            throw new IllegalArgumentException("Invalid in-flight MQTT message type: " + MqttMessageType.valueOf(type));
        }
    }

    /**
     * Convert MqttMessage to Map
     *
     * @param msg MqttMessage
     * @return Map
     */
    public static Map<String, String> mqttToMap(MqttMessage msg) {
        if (msg.fixedHeader().messageType() == MqttMessageType.PUBLISH) {
            Map<String, String> map = new HashMap<>();
            map.put("type", String.valueOf(MqttMessageType.PUBLISH.value()));
            map.put("retain", BooleanUtils.toString(msg.fixedHeader().isRetain(), "1", "0"));
            map.put("qos", String.valueOf(msg.fixedHeader().qosLevel().value()));
            map.put("dup", BooleanUtils.toString(msg.fixedHeader().isDup(), "1", "0"));
            map.put("topicName", ((MqttPublishVariableHeader) msg.variableHeader()).topicName());
            map.put("packetId", String.valueOf(((MqttPublishVariableHeader) msg.variableHeader()).messageId()));
            if (msg.payload() != null) try {
                map.put("payload", new String(((ByteBuf) msg.payload()).array(), "ISO-8859-1"));
            } catch (UnsupportedEncodingException ignore) {
            }
            return map;
        } else if (msg.fixedHeader().messageType() == MqttMessageType.PUBREL) {
            Map<String, String> map = new HashMap<>();
            map.put("type", String.valueOf(MqttMessageType.PUBREL.value()));
            map.put("qos", "1");
            map.put("packetId", String.valueOf(((MqttPublishVariableHeader) msg.variableHeader()).messageId()));
            return map;
        } else {
            throw new IllegalArgumentException("Invalid in-flight MQTT message type: " + msg.fixedHeader().messageType());
        }
    }

    public void init() {
        // open a new connection to a Redis server that treats keys and values as UTF-8 strings
        this.conn = this.client.connect();
    }

    public void destory() {
        // shutdown this client and close all open connections
        this.client.shutdown();
    }

    /**
     * Remove multiples keys
     * USE WITH CAUTION
     *
     * @param keys Keys
     * @return The number of keys have been removed
     */
    public RedisFuture<Long> removeKeys(String[] keys) {
        RedisAsyncCommands<String, String> commands = this.conn.async();
        return commands.del(keys);
    }

    /**
     * Get connected mqtt server nodes for the client
     * Client may have more than one connected nodes because:
     * Slow detection of tcp disconnected event
     * Clients use the the same client id
     *
     * @param clientId Client Id
     * @return MQTT Server Nodes
     */
    public RedisFuture<Set<String>> getConnectedNodes(String clientId) {
        RedisSetAsyncCommands<String, String> commands = this.conn.async();
        return commands.smembers(RedisKey.connectedNodes(clientId));
    }

    /**
     * Add connected mqtt server node for the client
     *
     * @param clientId Client Id
     * @param node     MQTT Server Node
     * @return The number of nodes that were added
     */
    public RedisFuture<Long> updateConnectedNodes(String clientId, String node) {
        RedisSetAsyncCommands<String, String> commands = this.conn.async();
        return commands.sadd(RedisKey.connectedNodes(clientId), node);
    }

    /**
     * Remove connected mqtt server node for the client
     *
     * @param clientId Client Id
     * @param node     MQTT Server Node
     * @return The number of nodes that were removed
     */
    public RedisFuture<Long> removeConnectedNodes(String clientId, String node) {
        RedisSetAsyncCommands<String, String> commands = this.conn.async();
        return commands.srem(RedisKey.connectedNodes(clientId), node);
    }

    /**
     * Is client (session) exist?
     *
     * @param clientId Client Id
     * @return 1 if exist
     */
    public RedisFuture<Long> isClientExist(String clientId) {
        RedisAsyncCommands<String, String> commands = this.conn.async();
        return commands.exists(new String[]{RedisKey.clientExist(clientId)});
    }

    /**
     * Set client (session) exist
     *
     * @param clientId Client Id
     * @return OK if was executed correctly
     */
    public RedisFuture<String> setClientExist(String clientId) {
        RedisAsyncCommands<String, String> commands = this.conn.async();
        return commands.set(RedisKey.clientExist(clientId), "1");
    }

    /**
     * Get all in-flight message's packet ids for the client
     * We separate packet ids with different clean session
     * Including:
     * QoS 1 and QoS 2 PUBLISH messages which have been sent to the Client, but have not been acknowledged.
     * QoS 0, QoS 1 and QoS 2 PUBLISH messages pending transmission to the Client.
     * QoS 2 PUBREL messages which have been sent from the Client, but have not been acknowledged.
     *
     * @param clientId     Client Id
     * @param cleanSession Clean Session
     * @return In-flight message's Packet Ids
     */
    public RedisFuture<List<String>> getInFlightIds(String clientId, boolean cleanSession) {
        RedisListAsyncCommands<String, String> commands = this.conn.async();
        return commands.lrange(RedisKey.inFlightList(clientId, cleanSession), 0, -1);
    }

    /**
     * Get specific in-flight message for the client
     *
     * @param clientId Client Id
     * @param packetId Packet Id
     * @return In-flight message in Map format
     */
    public RedisFuture<Map<String, String>> getInFlightMessage(String clientId, int packetId) {
        RedisHashAsyncCommands<String, String> commands = this.conn.async();
        return commands.hgetall(RedisKey.inFlightMessage(clientId, packetId));
    }

    /**
     * Remove specific in-flight message for the client
     * We separate packet ids with different clean session
     *
     * @param clientId     Client Id
     * @param cleanSession Clean Session
     * @param packetId     Packet Id
     * @return RedisFutures (Remove from the List, remove the Hash)
     */
    public List<RedisFuture> removeInFlightMessage(String clientId, boolean cleanSession, int packetId) {
        List<RedisFuture> list = new ArrayList<>();
        RedisListAsyncCommands<String, String> listCommands = this.conn.async();
        list.add(listCommands.lrem(RedisKey.inFlightList(clientId, cleanSession), 0, String.valueOf(packetId)));
        RedisAsyncCommands<String, String> commands = this.conn.async();
        list.add(commands.del(RedisKey.inFlightMessage(clientId, packetId)));
        return list;
    }

    /**
     * Remove all subscriptions for the client
     * We separate subscriptions with different clean session
     *
     * @param clientId     Client Id
     * @param cleanSession Clean Session
     * @return The number of subscriptions have been removed
     */
    public RedisFuture<Integer> removeSubscriptions(String clientId, boolean cleanSession) {
        RedisAsyncCommands<String, String> commands = this.conn.async();
        String[] keys = new String[]{};
        String[] values = new String[]{};
        return commands.evalsha("digest", ScriptOutputType.INTEGER, keys, values);
    }

    /**
     * Get the topic name's subscriptions
     *
     * @param topicLevels List of topic levels
     * @return Subscriptions
     */
    public RedisFuture<Map<String, String>> getTopicNameSubscriptions(List<String> topicLevels) {
        RedisHashAsyncCommands<String, String> commands = this.conn.async();
        return commands.hgetall(RedisKey.topicName(topicLevels));
    }

    /**
     * Update topic name subscription for the client
     *
     * @param topicLevels List of topic levels
     * @param clientId    Client Id
     * @param qos         Subscription QoS
     * @return True if new value was set; False if value was updated
     */
    public RedisFuture<Boolean> updateTopicNameSubscription(List<String> topicLevels, String clientId, String qos) {
        RedisHashAsyncCommands<String, String> commands = this.conn.async();
        return commands.hset(RedisKey.topicName(topicLevels), clientId, qos);
    }

    /***
     * Remove topic name subscription for the client
     *
     * @param topicLevels List of topic levels
     * @param clientId    Client Id
     * @return The number of fields that were removed
     */
    public RedisFuture<Long> removeTopicNameSubscription(List<String> topicLevels, String clientId) {
        RedisHashAsyncCommands<String, String> commands = this.conn.async();
        return commands.hdel(RedisKey.topicName(topicLevels), clientId);
    }

    /**
     * Get the topic filter's subscriptions
     *
     * @param topicLevels List of topic levels
     * @return Subscriptions
     */
    public RedisFuture<Map<String, String>> getTopicFilterSubscriptions(List<String> topicLevels) {
        RedisHashAsyncCommands<String, String> commands = this.conn.async();
        return commands.hgetall(RedisKey.topicFilter(topicLevels));
    }

    /**
     * Update topic filter subscription for the client
     *
     * @param topicLevels List of topic levels
     * @param clientId    Client Id
     * @param qos         Subscription QoS
     * @return RedisFutures (add to subscriptions, add to topic filter tree)
     */
    public List<RedisFuture> updateTopicFilterSubscription(List<String> topicLevels, String clientId, String qos) {
        List<RedisFuture> list = new ArrayList<>();
        RedisHashAsyncCommands<String, String> commands = this.conn.async();
        list.add(commands.hset(RedisKey.topicFilter(topicLevels), clientId, qos));
        for (int i = 0; i < topicLevels.size() - 1; i++) {
            list.add(commands.hincrby(RedisKey.topicFilterChild(topicLevels.subList(0, i + 1)), topicLevels.get(i + 1), 1));
        }
        return list;
    }

    /**
     * Remove topic filter subscription for the client
     *
     * @param topicLevels List of topic levels
     * @param clientId    Client Id
     * @return RedisFutures (remove from subscriptions, remove from topic filter tree)
     */
    public List<RedisFuture> removeTopicFilterSubscription(List<String> topicLevels, String clientId) {
        List<RedisFuture> list = new ArrayList<>();
        RedisHashAsyncCommands<String, String> commands = this.conn.async();
        list.add(commands.hdel(RedisKey.topicFilter(topicLevels), clientId));
        for (int i = 0; i < topicLevels.size() - 1; i++) {
            list.add(commands.hincrby(RedisKey.topicFilterChild(topicLevels.subList(0, i + 1)), topicLevels.get(i + 1), -1));
        }
        return list;
    }

    /**
     * Find the matching children from the topic filter tree
     *
     * @param topicLevels List of topic levels
     * @param index       Current match level
     * @return Possible matching children
     */
    public RedisFuture<List<String>> matchTopicFilterLevel(List<String> topicLevels, int index) {
        RedisHashAsyncCommands<String, String> commands = this.conn.async();
        if (index == topicLevels.size() - 1) {
            return commands.hmget(RedisKey.topicFilterChild(topicLevels.subList(0, index + 1)), topicLevels.get(index), "#");
        } else {
            return commands.hmget(RedisKey.topicFilterChild(topicLevels.subList(0, index + 1)), topicLevels.get(index), "+", "#");
        }
    }
}
