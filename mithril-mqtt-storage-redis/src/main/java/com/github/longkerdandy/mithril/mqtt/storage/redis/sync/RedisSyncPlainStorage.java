package com.github.longkerdandy.mithril.mqtt.storage.redis.sync;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.api.internal.Publish;
import com.github.longkerdandy.mithril.mqtt.storage.redis.RedisKey;
import com.github.longkerdandy.mithril.mqtt.storage.redis.RedisLua;
import com.github.longkerdandy.mithril.mqtt.util.Topics;
import com.lambdaworks.redis.*;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.*;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.longkerdandy.mithril.mqtt.storage.redis.util.Converter.*;
import static com.github.longkerdandy.mithril.mqtt.util.Topics.END;

/**
 * Synchronized Storage for Plain Redis setup
 * Single, Master Salve, Sentinel
 */
public class RedisSyncPlainStorage implements RedisSyncStorage {

    // A scalable thread-safe Redis client. Multiple threads may share one connection if they avoid
    // blocking and transactional operations such as BLPOP and MULTI/EXEC.
    private RedisClient client;
    // A thread-safe connection to a redis server. Multiple threads may share one StatefulRedisConnection
    private StatefulRedisConnection<String, String> conn;

    @SuppressWarnings("unused")
    protected RedisHashCommands<String, String> hash() {
        return this.conn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisKeyCommands<String, String> key() {
        return this.conn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisStringCommands<String, String> string() {
        return this.conn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisListCommands<String, String> list() {
        return this.conn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisSetCommands<String, String> set() {
        return this.conn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisSortedSetCommands<String, String> sortedSet() {
        return this.conn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisScriptingCommands<String, String> script() {
        return this.conn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisServerCommands<String, String> server() {
        return this.conn.sync();
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
    public void removeAllSessionState(String clientId) {
        removeSessionExist(clientId);
        removeAllSubscriptions(clientId);
        removeAllQoS2MessageId(clientId);
        removeAllInFlightMessage(clientId);
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
        return set().sadd(RedisKey.qos2Set(clientId), String.valueOf(packetId)) == 1;
    }

    @Override
    public boolean removeQoS2MessageId(String clientId, int packetId) {
        return set().srem(RedisKey.qos2Set(clientId), String.valueOf(packetId)) == 1;
    }

    @Override
    public void removeAllQoS2MessageId(String clientId) {
        key().del(RedisKey.qos2Set(clientId));
    }

    @Override
    public Map<String, MqttQoS> getTopicSubscriptions(List<String> topicLevels) {
        Map<String, MqttQoS> map = new HashMap<>();
        Map<String, String> subscriptions;
        if (Topics.isTopicFilter(topicLevels)) {
            subscriptions = hash().hgetall(RedisKey.topicFilter(topicLevels));
        } else {
            subscriptions = hash().hgetall(RedisKey.topicName(topicLevels));
        }
        if (subscriptions != null) {
            subscriptions.forEach((topic, qos) ->
                    map.put(topic, MqttQoS.valueOf(Integer.parseInt(qos))));
        }
        return map;
    }

    @Override
    public Map<String, MqttQoS> getClientSubscriptions(String clientId) {
        Map<String, MqttQoS> map = new HashMap<>();
        Map<String, String> subscriptions = hash().hgetall(RedisKey.subscription(clientId));
        if (subscriptions != null) {
            subscriptions.forEach((topic, qos) ->
                    map.put(topic, MqttQoS.valueOf(Integer.parseInt(qos))));
        }
        return map;
    }

    @Override
    public void updateSubscription(String clientId, List<String> topicLevels, MqttQoS qos) {
        List<String> keys = new ArrayList<>();
        List<String> argv = new ArrayList<>();
        if (Topics.isTopicFilter(topicLevels)) {
            // client's subscriptions
            keys.add(RedisKey.subscription(clientId));
            argv.add(String.join("/", topicLevels));
            argv.add(String.valueOf(qos.value()));
            // topic's subscribers
            keys.add(RedisKey.topicFilter(topicLevels));
            argv.add(clientId);
            argv.add(String.valueOf(qos.value()));
            // topic filter tree
            for (int i = 0; i < topicLevels.size(); i++) {
                keys.add(RedisKey.topicFilterChild(topicLevels.subList(0, i)));
                argv.add(topicLevels.get(i));
            }
            script().eval("local exist = redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])\n" +
                            "redis.call('HSET', KEYS[2], ARGV[3], ARGV[4])\n" +
                            "if exist == 1\n" +
                            "then\n" +
                            "   local length = table.getn(KEYS)\n" +
                            "   for i = 3, length do\n" +
                            "       redis.call('HINCRBY', KEYS[i], ARGV[i+2], 1)\n" +
                            "   end\n" +
                            "end\n" +
                            "return redis.status_reply('OK')",
                    ScriptOutputType.STATUS, keys.toArray(new String[keys.size()]), argv.toArray(new String[argv.size()]));
        } else {
            // client's subscriptions
            keys.add(RedisKey.subscription(clientId));
            argv.add(String.join("/", topicLevels));
            argv.add(String.valueOf(qos.value()));
            // topic's subscribers
            keys.add(RedisKey.topicName(topicLevels));
            argv.add(clientId);
            argv.add(String.valueOf(qos.value()));
            script().eval("redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])\n" +
                            "redis.call('HSET', KEYS[2], ARGV[3], ARGV[4])\n" +
                            "return redis.status_reply('OK')",
                    ScriptOutputType.STATUS, keys.toArray(new String[keys.size()]), argv.toArray(new String[argv.size()]));
        }
    }

    @Override
    public void removeSubscription(String clientId, List<String> topicLevels) {
        List<String> keys = new ArrayList<>();
        List<String> argv = new ArrayList<>();
        if (Topics.isTopicFilter(topicLevels)) {
            // client's subscriptions
            keys.add(RedisKey.subscription(clientId));
            argv.add(String.join("/", topicLevels));
            // topic's subscribers
            keys.add(RedisKey.topicFilter(topicLevels));
            argv.add(clientId);
            // topic filter tree
            for (int i = 0; i < topicLevels.size(); i++) {
                keys.add(RedisKey.topicFilterChild(topicLevels.subList(0, i)));
                argv.add(topicLevels.get(i));
            }
            script().eval("local exist = redis.call('HDEL', KEYS[1], ARGV[1])\n" +
                            "redis.call('HDEL', KEYS[2], ARGV[2])\n" +
                            "local length = table.getn(KEYS)\n" +
                            "if exist == 1\n" +
                            "then\n" +
                            "   for i = 3, length do\n" +
                            "       redis.call('HINCRBY', KEYS[i], ARGV[i], -1)\n" +
                            "   end\n" +
                            "end\n" +
                            "return redis.status_reply('OK')",
                    ScriptOutputType.STATUS, keys.toArray(new String[keys.size()]), argv.toArray(new String[argv.size()]));
        } else {
            // client's subscriptions
            keys.add(RedisKey.subscription(clientId));
            argv.add(String.join("/", topicLevels));
            // topic's subscribers
            keys.add(RedisKey.topicName(topicLevels));
            argv.add(clientId);
            script().eval("redis.call('HDEL', KEYS[1], ARGV[1])\n" +
                            "redis.call('HDEL', KEYS[2], ARGV[2])\n" +
                            "return redis.status_reply('OK')",
                    ScriptOutputType.STATUS, keys.toArray(new String[keys.size()]), argv.toArray(new String[argv.size()]));
        }
    }

    @Override
    public void removeAllSubscriptions(String clientId) {
        Map<String, String> map = hash().hgetall(RedisKey.subscription(clientId));
        if (map != null) {
            map.forEach((topic, qos) ->
                    removeSubscription(clientId, Topics.sanitize(topic)));
        }
    }

    /**
     * Get possible topic filter tree  sub nodes matching the topic
     * Topic Levels must been sanitized
     *
     * @param topicLevels List of topic levels
     * @param index       Current match level
     * @return Possible matching children
     */
    protected List<String> getMatchTopicFilter(List<String> topicLevels, int index) {
        if (index == topicLevels.size() - 1) {
            return hash().hmget(RedisKey.topicFilterChild(topicLevels.subList(0, index)), END, "#");
        } else {
            return hash().hmget(RedisKey.topicFilterChild(topicLevels.subList(0, index)), topicLevels.get(index), "#", "+");
        }
    }

    /**
     * Get and handle all topic filter subscriptions matching the topic
     * This is a recursion method
     * Topic Levels must been sanitized
     *
     * @param topicLevels List of topic levels
     * @param index       Current match level (use 0 if you have doubt)
     * @param map         RETURN VALUE! Subscriptions: Key - Client Id, Value - QoS
     */
    protected void getMatchSubscriptions(List<String> topicLevels, int index, Map<String, MqttQoS> map) {
        List<String> children = getMatchTopicFilter(topicLevels, index);

        // last one
        if (children.size() == 2) {
            int c = children.get(0) == null ? 0 : Integer.parseInt(children.get(0)); // char
            int s = children.get(1) == null ? 0 : Integer.parseInt(children.get(1)); // #
            if (c > 0) {
                Map<String, MqttQoS> subscriptions = getTopicSubscriptions(topicLevels);
                if (subscriptions != null) {
                    subscriptions.forEach((clientId, qos) -> {
                        if (qos.value() >= map.getOrDefault(clientId, MqttQoS.AT_MOST_ONCE).value()) {
                            map.put(clientId, qos);
                        }
                    });
                }
            }
            if (s > 0) {
                List<String> newTopicLevels = new ArrayList<>(topicLevels.subList(0, index));
                newTopicLevels.add("#");
                newTopicLevels.add(END);
                Map<String, MqttQoS> subscriptions = getTopicSubscriptions(newTopicLevels);
                if (subscriptions != null) {
                    subscriptions.forEach((clientId, qos) -> {
                        if (qos.value() >= map.getOrDefault(clientId, MqttQoS.AT_MOST_ONCE).value()) {
                            map.put(clientId, qos);
                        }
                    });
                }
            }
        }
        // not last one
        else if (children.size() == 3) {
            int c = children.get(0) == null ? 0 : Integer.parseInt(children.get(0)); // char
            int s = children.get(1) == null ? 0 : Integer.parseInt(children.get(1)); // #
            int p = children.get(2) == null ? 0 : Integer.parseInt(children.get(2)); // +
            if (c > 0) {
                getMatchSubscriptions(topicLevels, index + 1, map);
            }
            if (s > 0) {
                List<String> newTopicLevels = new ArrayList<>(topicLevels.subList(0, index));
                newTopicLevels.add("#");
                newTopicLevels.add(END);
                Map<String, MqttQoS> subscriptions = getTopicSubscriptions(newTopicLevels);
                if (subscriptions != null) {
                    subscriptions.forEach((clientId, qos) -> {
                        if (qos.value() >= map.getOrDefault(clientId, MqttQoS.AT_MOST_ONCE).value()) {
                            map.put(clientId, qos);
                        }
                    });
                }
            }
            if (p > 0) {
                List<String> newTopicLevels = new ArrayList<>(topicLevels);
                newTopicLevels.set(index, "+");
                getMatchSubscriptions(newTopicLevels, index + 1, map);
            }
        }
    }

    @Override
    public void getMatchSubscriptions(List<String> topicLevels, Map<String, MqttQoS> map) {
        if (Topics.isTopicFilter(topicLevels)) {
            throw new IllegalArgumentException("it must be topic name not topic filter");
        }

        // topic name
        Map<String, MqttQoS> subscriptions = getTopicSubscriptions(topicLevels);
        if (subscriptions != null) {
            map.putAll(subscriptions);
        }

        // topic filter
        getMatchSubscriptions(topicLevels, 0, map);
    }

    @Override
    public void addRetainMessage(List<String> topicLevels, InternalMessage<Publish> msg) {
        int retainId = Math.toIntExact(script().eval(RedisLua.INCRLIMIT, ScriptOutputType.INTEGER, new String[]{RedisKey.nextRetainId(topicLevels)}, new String[]{"65535"}));
        Map<String, String> map = internalToMap(msg);
        String[] keys = new String[]{RedisKey.topicRetainList(topicLevels), RedisKey.topicRemainMessage(topicLevels, retainId)};
        String[] argv = ArrayUtils.addAll(new String[]{String.valueOf(retainId)}, mapToArray(map));
        script().eval("redis.call('RPUSH', KEYS[1], ARGV[1])\n" +
                        "redis.call('HMSET', KEYS[2], unpack(ARGV, 2))\n" +
                        "return redis.status_reply('OK')",
                ScriptOutputType.STATUS, keys, argv);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<InternalMessage<Publish>> getAllRetainMessages(List<String> topicLevels) {
        List<InternalMessage<Publish>> r = new ArrayList<>();
        List<String> ids = list().lrange(RedisKey.topicRetainList(topicLevels), 0, -1);
        if (ids != null) {
            ids.forEach(retainId -> {
                InternalMessage<Publish> m = mapToInternal(hash().hgetall(RedisKey.topicRemainMessage(topicLevels, Integer.parseInt(retainId))));
                if (m != null) r.add(m);
            });
        }
        return r;
    }

    @Override
    public void removeAllRetainMessage(List<String> topicLevels) {
        List<String> ids = list().lrange(RedisKey.topicRetainList(topicLevels), 0, -1);
        if (ids != null) {
            ids.forEach(retainId -> {
                String[] keys = new String[]{RedisKey.topicRetainList(topicLevels), RedisKey.topicRemainMessage(topicLevels, Integer.parseInt(retainId))};
                String[] argv = new String[]{String.valueOf(retainId)};
                script().eval("redis.call('LREM', KEYS[1], 0, ARGV[1])\n" +
                                "redis.call('DEL', KEYS[2])\n" +
                                "return redis.status_reply('OK')",
                        ScriptOutputType.STATUS, keys, argv);
            });
        }
    }
}
