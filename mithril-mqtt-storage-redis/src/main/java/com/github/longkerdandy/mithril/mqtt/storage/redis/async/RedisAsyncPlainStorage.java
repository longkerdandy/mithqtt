package com.github.longkerdandy.mithril.mqtt.storage.redis.async;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.storage.redis.RedisKey;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.ScriptOutputType;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.async.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;

import java.util.Map;

import static com.github.longkerdandy.mithril.mqtt.storage.redis.util.Converter.internalToMap;
import static com.github.longkerdandy.mithril.mqtt.storage.redis.util.Converter.mapToArray;

/**
 * Asynchronous Storage for Plain Redis setup
 * Single, Master Salve, Sentinel
 */
public class RedisAsyncPlainStorage implements RedisAsyncStorage {

    // A scalable thread-safe Redis client. Multiple threads may share one connection if they avoid
    // blocking and transactional operations such as BLPOP and MULTI/EXEC.
    private RedisClient client;
    // A thread-safe connection to a redis server. Multiple threads may share one StatefulRedisConnection
    private StatefulRedisConnection<String, String> conn;

    @SuppressWarnings("unused")
    protected RedisHashAsyncCommands<String, String> hash() {
        return this.conn.async();
    }

    @SuppressWarnings("unused")
    protected RedisKeyAsyncCommands<String, String> key() {
        return this.conn.async();
    }

    @SuppressWarnings("unused")
    protected RedisStringAsyncCommands<String, String> string() {
        return this.conn.async();
    }

    @SuppressWarnings("unused")
    protected RedisListAsyncCommands<String, String> list() {
        return this.conn.async();
    }

    @SuppressWarnings("unused")
    protected RedisSetAsyncCommands<String, String> set() {
        return this.conn.async();
    }

    @SuppressWarnings("unused")
    protected RedisSortedSetAsyncCommands<String, String> sortedSet() {
        return this.conn.async();
    }

    @SuppressWarnings("unused")
    protected RedisScriptingAsyncCommands<String, String> script() {
        return this.conn.async();
    }

    @SuppressWarnings("unused")
    protected RedisServerAsyncCommands<String, String> server() {
        return this.conn.async();
    }

    @Override
    public void init(RedisURI redisURI) {
        // open a new connection to a Redis server that treats keys and values as UTF-8 strings
        this.client = RedisClient.create(redisURI);
        this.conn = this.client.connect();
    }

    @Override
    public void destroy() {
        // shutdown this client and close all open connections
        if (this.conn != null) this.conn.close();
        if (this.client != null) this.client.shutdown();
    }

    @Override
    public RedisFuture<String> getSessionExist(String clientId) {
        return string().get(RedisKey.session(clientId));
    }

    @Override
    public RedisFuture<Long> removeQoS2MessageId(String clientId, int packetId) {
        return set().srem(RedisKey.qos2Set(clientId), String.valueOf(packetId));
    }

    @Override
    public RedisFuture<String> addInFlightMessage(String clientId, int packetId, InternalMessage msg, boolean dup) {
        Map<String, String> map = internalToMap(msg);
        map.put("dup", BooleanUtils.toString(dup, "1", "0"));
        String[] keys = new String[]{RedisKey.inFlightList(clientId), RedisKey.inFlightMessage(clientId, packetId)};
        String[] argv = ArrayUtils.addAll(new String[]{String.valueOf(packetId)}, mapToArray(map));
        return script().eval("redis.call('RPUSH', KEYS[1], ARGV[1])\n" +
                        "redis.call('HMSET', KEYS[2], unpack(ARGV, 2))\n" +
                        "return redis.status_reply('OK')",
                ScriptOutputType.STATUS, keys, argv);
    }

    @Override
    public RedisFuture<String> removeInFlightMessage(String clientId, int packetId) {
        String[] keys = new String[]{RedisKey.inFlightList(clientId), RedisKey.inFlightMessage(clientId, packetId)};
        String[] argv = new String[]{String.valueOf(packetId)};
        return script().eval("redis.call('LREM', KEYS[1], 0, ARGV[1])\n" +
                        "redis.call('DEL', KEYS[2])\n" +
                        "return redis.status_reply('OK')",
                ScriptOutputType.STATUS, keys, argv);
    }
}
