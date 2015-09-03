package com.github.longkerdandy.mithril.mqtt.storage.redis;

import com.github.longkerdandy.mithril.mqtt.entity.InFlightMessage;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.ScriptOutputType;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import com.lambdaworks.redis.api.async.RedisHashAsyncCommands;
import com.lambdaworks.redis.api.async.RedisListAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public void init() {
        // open a new connection to a Redis server that treats keys and values as UTF-8 strings
        this.conn = this.client.connect();
    }

    public void destory() {
        // shutdown this client and close all open connections
        this.client.shutdown();
    }

    public RedisFuture<Long> removeKeys(String[] keys) {
        RedisAsyncCommands<String, String> commands = this.conn.async();
        return commands.del(keys);
    }

    /**
     * Get all in-flight message's packetIds for the client
     * Including£º
     * QoS 1 and QoS 2 PUBLISH messages which have been sent to the Client, but have not been acknowledged.
     * QoS 0, QoS 1 and QoS 2 PUBLISH messages pending transmission to the Client.
     * QoS 2 PUBREL messages which have been sent from the Client, but have not been acknowledged.
     *
     * @param clientId Client Id
     * @return In-flight message's Packet Ids
     */
    public RedisFuture<List<String>> getInFlightIds(String clientId) {
        RedisListAsyncCommands<String, String> commands = this.conn.async();
        return commands.lrange(RedisKey.inFlightList(clientId), 0, -1);
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
     *
     * @param clientId Client Id
     * @param packetId Packet Id
     * @return Two RedisFutures (Remove from the List, remove the Hash)
     */
    public List<RedisFuture> removeInFlightMessage(String clientId, int packetId) {
        List<RedisFuture> list = new ArrayList<>();
        RedisListAsyncCommands<String, String> listCommands = this.conn.async();
        list.add(listCommands.lrem(RedisKey.inFlightList(clientId), 0, String.valueOf(packetId)));
        RedisAsyncCommands<String, String> commands = this.conn.async();
        list.add(commands.del(RedisKey.inFlightMessage(clientId, packetId)));
        return list;
    }

    /**
     * Remove all subscriptions for the client
     *
     * @param clientId Client Id
     * @return RedisFuture indicate the number of subscriptions have been removed
     */
    public RedisFuture<Integer> removeSubscriptions(String clientId) {
        RedisAsyncCommands<String, String> commands = this.conn.async();
        String[] keys = new String[] {};
        String[] values = new String[] {};
        return commands.evalsha("digest", ScriptOutputType.INTEGER, keys, values);
    }

    /**
     * Convert Redis Hash(Map) to InFlightMessage
     *
     * @param map Redis Hash
     * @return InFlightMessage
     */
    public static InFlightMessage toInFlight(Map<String, String> map) {
        InFlightMessage msg = new InFlightMessage();
        msg.setType(Integer.parseInt(map.get("type")));
        msg.setRetain(Boolean.parseBoolean(map.getOrDefault("retain", "false")));
        msg.setQos(Integer.parseInt(map.getOrDefault("qos", "0")));
        msg.setDup(Boolean.parseBoolean(map.getOrDefault("dup", "false")));
        msg.setTopicName(map.get("topicName"));
        msg.setPacketId(Integer.parseInt(map.get("packetId")));
        String payload = map.get("payload");
        if (payload != null) try {
            msg.setPayload(payload.getBytes("ISO-8859-1"));
        } catch (UnsupportedEncodingException ignore) {
        }
        return msg;
    }

    /**
     * Convert InFlightMessage to Redis Hash(Map)
     *
     * @param msg InFlightMessage
     * @return Redis Hash
     */
    public static Map<String, String> fromInFlight(InFlightMessage msg) {
        Map<String, String> map = new HashMap<>();
        map.put("type", String.valueOf(msg.getType()));
        map.put("retain", String.valueOf(msg.isRetain()));
        map.put("qos", String.valueOf(msg.getQos()));
        map.put("dup", String.valueOf(msg.isDup()));
        map.put("topicName", msg.getTopicName());
        map.put("packetId", String.valueOf(msg.getPacketId()));
        if (msg.getPayload() != null) try {
            map.put("payload", new String(msg.getPayload(), "ISO-8859-1"));
        } catch (UnsupportedEncodingException ignore) {
        }
        return map;
    }
}
