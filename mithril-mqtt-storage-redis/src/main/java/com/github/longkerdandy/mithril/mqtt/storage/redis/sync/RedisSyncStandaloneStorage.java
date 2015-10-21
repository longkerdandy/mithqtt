package com.github.longkerdandy.mithril.mqtt.storage.redis.sync;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.api.internal.Publish;
import com.github.longkerdandy.mithril.mqtt.storage.redis.RedisKey;
import com.github.longkerdandy.mithril.mqtt.storage.redis.RedisLua;
import com.lambdaworks.redis.*;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.*;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.longkerdandy.mithril.mqtt.storage.redis.util.Converter.*;

/**
 * Synchronized Storage for Standalone Redis setup
 */
public class RedisSyncStandaloneStorage implements RedisSyncStorage {

    // A scalable thread-safe Redis client. Multiple threads may share one connection if they avoid
    // blocking and transactional operations such as BLPOP and MULTI/EXEC.
    private RedisClient client;
    // A thread-safe connection to a redis server. Multiple threads may share one StatefulRedisConnection
    private StatefulRedisConnection<String, String> conn;

    public RedisSyncStandaloneStorage(RedisURI redisURI) {
        this.client = RedisClient.create(redisURI);
    }

    protected RedisHashCommands<String, String> hash() {
        return this.conn.sync();
    }

    protected RedisKeyCommands<String, String> key() {
        return this.conn.sync();
    }

    protected RedisStringCommands<String, String> string() {
        return this.conn.sync();
    }

    protected RedisListCommands<String, String> list() {
        return this.conn.sync();
    }

    protected RedisSetCommands<String, String> set() {
        return this.conn.sync();
    }

    protected RedisSortedSetCommands<String, String> sortedSet() {
        return this.conn.sync();
    }

    protected RedisScriptingCommands<String, String> script() {
        return this.conn.sync();
    }

    protected RedisServerCommands<String, String> server() {
        return this.conn.sync();
    }

    public void init() {
        // open a new connection to a Redis server that treats keys and values as UTF-8 strings
        this.conn = this.client.connect();
    }

    public void destroy() {
        // shutdown this client and close all open connections
        if (this.conn != null) this.conn.close();
        this.client.shutdown();
    }

    @Override
    public ValueScanCursor<String> getConnectedClients(String node, String cursor, long count) {
        return set().sscan(RedisKey.connectedClients(node), ScanCursor.of(cursor), ScanArgs.Builder.limit(count));
    }

    @Override
    public String getConnectedNode(String clientId) {
        return string().get(RedisKey.connectedNode(clientId));
    }

    @Override
    public String updateConnectedNode(String clientId, String node) {
        String[] keys = new String[]{RedisKey.connectedClients(node), RedisKey.connectedNode(clientId)};
        String[] argv = new String[]{clientId, node};
        return script().eval("redis.call('SADD', KEYS[1], ARGV[1])\n" +
                        "return redis.call('GETSET', KEYS[2], ARGV[2])",
                ScriptOutputType.VALUE, keys, argv);
    }

    @Override
    public boolean removeConnectedNode(String clientId, String node) {
        String[] keys = new String[]{RedisKey.connectedClients(node), RedisKey.connectedNode(clientId)};
        String[] argv = new String[]{clientId, node};
        long r = script().eval("redis.call('SREM', KEYS[1], ARGV[1])\n" +
                        "if ARGV[2] == redis.call('GET', KEYS[2])\n" +
                        "then\n" +
                        "   redis.call('DEL', KEYS[2])\n" +
                        "   return 1\n" +
                        "end\n" +
                        "return 0",
                ScriptOutputType.INTEGER, keys, argv);
        return r == 1;
    }

    @Override
    public int getSessionExist(String clientId) {
        String r = string().get(RedisKey.session(clientId));
        if (r != null) return Integer.parseInt(r);
        else return -1;
    }

    @Override
    public void updateSessionExist(String clientId, boolean cleanSession) {
        string().set(RedisKey.session(clientId), BooleanUtils.toString(cleanSession, "1", "0"));
    }

    @Override
    public boolean removeSessionExist(String clientId) {
        return key().del(RedisKey.session(clientId)) == 1;
    }

    @Override
    public void removeAllSessionState(String clientId, String node) {

    }

    @Override
    public int getNextPacketId(String clientId) {
        String[] keys = new String[]{RedisKey.nextPacketId(clientId)};
        String[] values = new String[]{"65535"};
        return Math.toIntExact(script().eval(RedisLua.INCRLIMIT, ScriptOutputType.INTEGER, keys, values));
    }

    @Override
    public InternalMessage getInFlightMessage(String clientId, int packetId) {
        return mapToInternal(hash().hgetall(RedisKey.inFlightMessage(clientId, packetId)));
    }

    @Override
    public void addInFlightMessage(String clientId, int packetId, InternalMessage msg, boolean dup) {
        Map<String, String> map = internalToMap(msg);
        map.put("dup", BooleanUtils.toString(dup, "1", "0"));
        String[] keys = new String[]{RedisKey.inFlightList(clientId), RedisKey.inFlightMessage(clientId, packetId)};
        String[] argv = ArrayUtils.addAll(new String[]{String.valueOf(packetId)}, mapToArray(map));
        script().eval("redis.call('RPUSH', KEYS[1], ARGV[1])\n" +
                        "redis.call('HMSET', KEYS[2], unpack(ARGV, 2))\n" +
                        "return redis.status_reply('OK')",
                ScriptOutputType.STATUS, keys, argv);
    }

    @Override
    public void removeInFlightMessage(String clientId, int packetId) {
        String[] keys = new String[]{RedisKey.inFlightList(clientId), RedisKey.inFlightMessage(clientId, packetId)};
        String[] argv = new String[]{String.valueOf(packetId)};
        script().eval("redis.call('LREM', KEYS[1], 0, ARGV[1])\n" +
                        "redis.call('DEL', KEYS[2])\n" +
                        "return redis.status_reply('OK')",
                ScriptOutputType.STATUS, keys, argv);
    }

    @Override
    public List<InternalMessage> getAllInFlightMessages(String clientId) {
        List<InternalMessage> r = new ArrayList<>();
        List<String> ids = list().lrange(RedisKey.inFlightList(clientId), 0, -1);
        if (ids != null) {
            ids.forEach(packetId -> {
                InternalMessage m = getInFlightMessage(clientId, Integer.parseInt(packetId));
                if (m != null) r.add(m);
            });
        }
        return r;
    }

    @Override
    public void removeAllInFlightMessage(String clientId) {
        List<String> ids = list().lrange(RedisKey.inFlightList(clientId), 0, -1);
        if (ids != null) {
            ids.forEach(packetId ->
                    removeInFlightMessage(clientId, Integer.parseInt(packetId)));
        }
    }

    @Override
    public boolean addQoS2MessageId(String clientId, int packetId) {
        return false;
    }

    @Override
    public boolean removeQoS2MessageId(String clientId, int packetId) {
        return false;
    }

    @Override
    public void removeAllQoS2MessageId(String clientId) {

    }

    @Override
    public void updateSubscription(String clientId, List<String> topicLevels, MqttQoS qos) {

    }

    @Override
    public void removeSubscription(String clientId, List<String> topicLevels) {

    }

    @Override
    public void removeAllSubscriptions(String clientId) {

    }

    @Override
    public void getMatchSubscriptions(List<String> topicLevels, Map<String, MqttQoS> map) {

    }

    @Override
    public InternalMessage<Publish> getRetainMessage(List<String> topicLevels, int packetId) {
        return null;
    }

    @Override
    public void addRetainMessage(List<String> topicLevels, int packetId, InternalMessage<Publish> msg) {

    }

    @Override
    public List<InternalMessage<Publish>> getAllRetainMessages(List<String> topicLevels) {
        return null;
    }

    @Override
    public void removeAllRetainMessage(List<String> topicLevels) {

    }
}
