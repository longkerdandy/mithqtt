package com.github.longkerdandy.mithqtt.storage.redis.sync;

import com.github.longkerdandy.mithqtt.storage.redis.RedisLua;
import com.github.longkerdandy.mithqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithqtt.api.internal.Publish;
import com.github.longkerdandy.mithqtt.storage.redis.RedisKey;
import com.github.longkerdandy.mithqtt.util.Topics;
import com.lambdaworks.redis.*;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.*;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.redisson.Config;
import org.redisson.Redisson;
import org.redisson.RedissonClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import static com.github.longkerdandy.mithqtt.storage.redis.util.Converter.internalToMap;
import static com.github.longkerdandy.mithqtt.storage.redis.util.Converter.mapToInternal;
import static com.github.longkerdandy.mithqtt.util.Topics.END;

/**
 * Synchronized Storage for Single Redis setup
 */
public class RedisSyncSingleStorage implements RedisSyncStorage {

    // Main infrastructure class allows to get access to all Redisson objects on top of Redis server
    protected RedissonClient redisson;

    // Max in-flight queue size per client
    protected int inFlightQueueSize;
    // Max QoS 2 ids queue size per client
    protected int qos2QueueSize;
    // Max retain queue size per topic
    protected int retainQueueSize;

    // A scalable thread-safe Redis client. Multiple threads may share one connection if they avoid
    // blocking and transactional operations such as BLPOP and MULTI/EXEC.
    private RedisClient lettuce;
    // A thread-safe connection to a redis server. Multiple threads may share one StatefulRedisConnection
    private StatefulRedisConnection<String, String> lettuceConn;

    @SuppressWarnings("unused")
    protected RedisHashCommands<String, String> hash() {
        return this.lettuceConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisKeyCommands<String, String> key() {
        return this.lettuceConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisStringCommands<String, String> string() {
        return this.lettuceConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisListCommands<String, String> list() {
        return this.lettuceConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisSetCommands<String, String> set() {
        return this.lettuceConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisSortedSetCommands<String, String> sortedSet() {
        return this.lettuceConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisScriptingCommands<String, String> script() {
        return this.lettuceConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisServerCommands<String, String> server() {
        return this.lettuceConn.sync();
    }

    @Override
    public void init(AbstractConfiguration config) {
        if (!config.getString("redis.type").equals("single")) {
            throw new IllegalStateException("RedisSyncSingleStorage class can only be used with single redis setup, but redis.type value is " + config.getString("redis.type"));
        }

        List<String> address = parseRedisAddress(config.getString("redis.address"), 6379);
        int databaseNumber = config.getInt("redis.database", 0);
        String password = StringUtils.isNotEmpty(config.getString("redis.password")) ? config.getString("redis.password") + "@" : "";

        // lettuce
        RedisURI lettuceURI = RedisURI.create("redis://" + password + address.get(0) + "/" + databaseNumber);
        this.lettuce = RedisClient.create(lettuceURI);
        this.lettuceConn = this.lettuce.connect();

        // redisson
        Config redissonConfig = new Config();
        redissonConfig.useSingleServer()
                .setAddress(address.get(0))
                .setDatabase(databaseNumber)
                .setPassword(StringUtils.isNotEmpty(password) ? password : null);
        this.redisson = Redisson.create(redissonConfig);

        // params
        initParams(config);
    }

    @Override
    public void destroy() {
        // shutdown this client and close all open connections
        if (this.lettuceConn != null) this.lettuceConn.close();
        if (this.lettuce != null) this.lettuce.shutdown();
        if (this.redisson != null) this.redisson.shutdown();
    }

    /**
     * Parse address string to a List of host:port String
     *
     * @param address Address String
     * @return List of host:port String
     */
    protected List<String> parseRedisAddress(String address, int defaultPort) {
        List<String> list = new ArrayList<>();
        String[] array = address.split(",");
        for (String s : array) {
            if (!s.contains(":"))
                s = s + ":" + defaultPort;
            list.add(s);
        }
        return list;
    }

    /**
     * Initialize MQTT parameters
     *
     * @param config Redis Configuration
     */
    protected void initParams(AbstractConfiguration config) {
        this.inFlightQueueSize = config.getInt("mqtt.inflight.queue.size", 0);
        this.qos2QueueSize = config.getInt("mqtt.qos2.queue.size", 0);
        this.retainQueueSize = config.getInt("mqtt.retain.queue.size", 0);
    }

    @Override
    public Lock getLock(String name) {
        return this.redisson.getLock(name);
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
        set().sadd(RedisKey.connectedClients(node), clientId);
        return string().getset(RedisKey.connectedNode(clientId), node);
    }

    @Override
    public boolean removeConnectedNode(String clientId, String node) {
        set().srem(RedisKey.connectedClients(node), clientId);
        long r = script().eval(RedisLua.CHECKDEL, ScriptOutputType.INTEGER, new String[]{RedisKey.connectedNode(clientId)}, node);
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
        return Math.toIntExact(script().eval(RedisLua.INCRLIMIT, ScriptOutputType.INTEGER, new String[]{RedisKey.nextPacketId(clientId)}, "65535"));
    }

    @Override
    public InternalMessage getInFlightMessage(String clientId, int packetId) {
        InternalMessage m = mapToInternal(hash().hgetall(RedisKey.inFlightMessage(clientId, packetId)));
        if (m == null) removeInFlightMessage(clientId, packetId);
        return m;
    }

    @Override
    public void addInFlightMessage(String clientId, int packetId, InternalMessage msg, boolean dup) {
        Map<String, String> map = internalToMap(msg);
        map.put("dup", BooleanUtils.toString(dup, "1", "0"));
        String r = script().eval(RedisLua.RPUSHLIMIT, ScriptOutputType.VALUE, new String[]{RedisKey.inFlightList(clientId)}, String.valueOf(packetId), String.valueOf(this.inFlightQueueSize));
        if (r != null) key().del(RedisKey.inFlightMessage(clientId, Integer.parseInt(r)));
        hash().hmset(RedisKey.inFlightMessage(clientId, packetId), map);
    }

    @Override
    public void addInFlightMessage(String clientId, int packetId, InternalMessage msg, boolean dup, long ttl) {
        addInFlightMessage(clientId, packetId, msg, dup);
        key().expire(RedisKey.inFlightMessage(clientId, packetId), ttl);
    }

    @Override
    public void removeInFlightMessage(String clientId, int packetId) {
        list().lrem(RedisKey.inFlightList(clientId), 0, String.valueOf(packetId));
        key().del(RedisKey.inFlightMessage(clientId, packetId));
    }

    @Override
    public List<InternalMessage> getAllInFlightMessages(String clientId) {
        List<InternalMessage> r = new ArrayList<>();
        List<String> ids = list().lrange(RedisKey.inFlightList(clientId), 0, -1);
        if (ids != null) {
            ids.forEach(packetId -> {
                InternalMessage m = getInFlightMessage(clientId, Integer.parseInt(packetId));
                if (m != null) r.add(m);
                else removeInFlightMessage(clientId, Integer.parseInt(packetId));
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
        long r = script().eval(RedisLua.ZADDLIMIT, ScriptOutputType.INTEGER,
                new String[]{RedisKey.qos2Set(clientId)},
                String.valueOf(System.currentTimeMillis()),
                String.valueOf(packetId),
                String.valueOf(this.qos2QueueSize));
        return r == 1;
    }

    @Override
    public boolean removeQoS2MessageId(String clientId, int packetId) {
        return sortedSet().zrem(RedisKey.qos2Set(clientId), String.valueOf(packetId)) == 1;
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
        if (Topics.isTopicFilter(topicLevels)) {
            boolean b1 = hash().hset(RedisKey.subscription(clientId), String.join("/", topicLevels), String.valueOf(qos.value()));
            boolean b2 = hash().hset(RedisKey.topicFilter(topicLevels), clientId, String.valueOf(qos.value()));
            if (b1 && b2) {
                List<String> keys = new ArrayList<>();
                List<String> argv = new ArrayList<>();
                // topic filter tree
                for (int i = 0; i < topicLevels.size(); i++) {
                    keys.add(RedisKey.topicFilterChild(topicLevels.subList(0, i)));
                    argv.add(topicLevels.get(i));
                }
                script().eval("local length = table.getn(KEYS)\n" +
                                "for i = 1, length do\n" +
                                "   redis.call('HINCRBY', KEYS[i], ARGV[i], 1)\n" +
                                "end\n" +
                                "return redis.status_reply('OK')",
                        ScriptOutputType.STATUS, keys.toArray(new String[keys.size()]), argv.toArray(new String[argv.size()]));
            }
        } else {
            hash().hset(RedisKey.subscription(clientId), String.join("/", topicLevels), String.valueOf(qos.value()));
            hash().hset(RedisKey.topicName(topicLevels), clientId, String.valueOf(qos.value()));
        }
    }

    @Override
    public void removeSubscription(String clientId, List<String> topicLevels) {
        if (Topics.isTopicFilter(topicLevels)) {
            long b1 = hash().hdel(RedisKey.subscription(clientId), String.join("/", topicLevels));
            long b2 = hash().hdel(RedisKey.topicFilter(topicLevels), clientId);
            if (b1 == 1 && b2 == 1) {
                List<String> keys = new ArrayList<>();
                List<String> argv = new ArrayList<>();
                // topic filter tree
                for (int i = 0; i < topicLevels.size(); i++) {
                    keys.add(RedisKey.topicFilterChild(topicLevels.subList(0, i)));
                    argv.add(topicLevels.get(i));
                }
                script().eval("local length = table.getn(KEYS)\n" +
                                "for i = 1, length do\n" +
                                "   local count = redis.call('HINCRBY', KEYS[i], ARGV[i], -1)\n" +
                                "   if count == 0\n" +
                                "   then\n" +
                                "       redis.call('HDEL', KEYS[i], ARGV[i])\n" +
                                "   end\n" +
                                "end\n" +
                                "return redis.status_reply('OK')",
                        ScriptOutputType.STATUS, keys.toArray(new String[keys.size()]), argv.toArray(new String[argv.size()]));
            }
        } else {
            hash().hdel(RedisKey.subscription(clientId), String.join("/", topicLevels));
            hash().hdel(RedisKey.topicName(topicLevels), clientId);
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
     * Get possible topic filter tree sub nodes matching the topic
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
    public int addRetainMessage(List<String> topicLevels, InternalMessage<Publish> msg) {
        // retainId
        int retainId = Math.toIntExact(script().eval(RedisLua.INCRLIMIT, ScriptOutputType.INTEGER, new String[]{RedisKey.nextRetainId(topicLevels)}, new String[]{"65535"}));

        // retain's message list
        String r = script().eval(RedisLua.RPUSHLIMIT, ScriptOutputType.VALUE, new String[]{RedisKey.topicRetainList(topicLevels)}, String.valueOf(retainId), String.valueOf(this.retainQueueSize));
        if (r != null) {
            List<String> keys = new ArrayList<>();
            List<String> argv = new ArrayList<>();
            for (int i = 0; i < topicLevels.size(); i++) {
                keys.add(RedisKey.topicRetainChild(topicLevels.subList(0, i)));
                argv.add(topicLevels.get(i));
            }
            script().eval("local length = table.getn(KEYS)\n" +
                            "for i = 1, length do\n" +
                            "   local count = redis.call('HINCRBY', KEYS[i], ARGV[i], -1)\n" +
                            "   if count == 0\n" +
                            "   then\n" +
                            "       redis.call('HDEL', KEYS[i], ARGV[i])\n" +
                            "   end\n" +
                            "end\n" +
                            "return redis.status_reply('OK')",
                    ScriptOutputType.STATUS, keys.toArray(new String[keys.size()]), argv.toArray(new String[argv.size()]));

            key().del(RedisKey.topicRemainMessage(topicLevels, retainId));
        }

        // retain tree
        List<String> keys = new ArrayList<>();
        List<String> argv = new ArrayList<>();
        for (int i = 0; i < topicLevels.size(); i++) {
            keys.add(RedisKey.topicRetainChild(topicLevels.subList(0, i)));
            argv.add(topicLevels.get(i));
        }
        script().eval("local length = table.getn(KEYS)\n" +
                        "for i = 1, length do\n" +
                        "    redis.call('HINCRBY', KEYS[i], ARGV[i], 1)\n" +
                        "end\n" +
                        "return redis.status_reply('OK')",
                ScriptOutputType.STATUS, keys.toArray(new String[keys.size()]), argv.toArray(new String[argv.size()]));

        // retain message
        hash().hmset(RedisKey.topicRemainMessage(topicLevels, retainId), internalToMap(msg));

        return retainId;
    }

    /**
     * Remove the specific retain message
     *
     * @param topicLevels Topic Levels
     * @param retainId    Retain Id
     */
    protected void removeRetainMessage(List<String> topicLevels, int retainId) {
        // retain's message list
        long b = list().lrem(RedisKey.topicRetainList(topicLevels), 1, String.valueOf(retainId));

        // retain tree
        if (b == 1) {
            List<String> keys = new ArrayList<>();
            List<String> argv = new ArrayList<>();
            for (int i = 0; i < topicLevels.size(); i++) {
                keys.add(RedisKey.topicRetainChild(topicLevels.subList(0, i)));
                argv.add(topicLevels.get(i));
            }
            script().eval("local length = table.getn(KEYS)\n" +
                            "for i = 1, length do\n" +
                            "   local count = redis.call('HINCRBY', KEYS[i], ARGV[i], -1)\n" +
                            "   if count == 0\n" +
                            "   then\n" +
                            "       redis.call('HDEL', KEYS[i], ARGV[i])\n" +
                            "   end\n" +
                            "end\n" +
                            "return redis.status_reply('OK')",
                    ScriptOutputType.STATUS, keys.toArray(new String[keys.size()]), argv.toArray(new String[argv.size()]));
        }

        // retain message
        key().del(RedisKey.topicRemainMessage(topicLevels, retainId));
    }

    @Override
    public void removeAllRetainMessage(List<String> topicLevels) {
        List<String> ids = list().lrange(RedisKey.topicRetainList(topicLevels), 0, -1);
        if (ids != null) {
            ids.forEach(retainId ->
                    removeRetainMessage(topicLevels, Integer.parseInt(retainId)));
        }
    }

    /**
     * Get all retain message topics matching the specific prefix
     * This used to match topic wildcard '#'
     * This is a recursion method
     * Topic Levels must been sanitized
     *
     * @param topicLevels Prefix of retain message
     * @param list        RETURN VALUE! List of retain message topics
     */
    protected void getMatchRetainPrefix(List<String> topicLevels, List<List<String>> list) {
        Map<String, String> nodes = hash().hgetall(RedisKey.topicRetainChild(topicLevels));
        if (nodes != null) {
            nodes.forEach((node, count) -> {
                int c = Integer.parseInt(count);
                if (c > 0) {
                    List<String> l = new ArrayList<>(topicLevels);
                    l.add(node);
                    if (node.equals(Topics.END)) {
                        list.add(l);
                    } else {
                        getMatchRetainPrefix(l, list);
                    }
                }
            });
        }
    }

    /**
     * Get all retain message topics matching the topic filter
     * This is a recursion method
     * Topic Levels must been sanitized
     *
     * @param topicLevels Topic Filter
     * @param index       Current match level (use 0 if you have doubt)
     * @param list        RETURN VALUE! List of retain message topics
     */
    protected void getMatchRetainMessages(List<String> topicLevels, int index, List<List<String>> list) {
        String level = topicLevels.get(index);

        switch (level) {
            case "#":
                List<String> t1 = new ArrayList<>(topicLevels.subList(0, index));
                getMatchRetainPrefix(t1, list);
                break;
            case "+":
                Map<String, String> nodes = hash().hgetall(RedisKey.topicRetainChild(topicLevels.subList(0, index)));
                if (nodes != null) {
                    nodes.forEach((node, count) -> {
                        if (!node.equals(Topics.END) && Integer.parseInt(count) > 0) {
                            if (node.equals(Topics.END)) {
                                List<String> t2 = new ArrayList<>(topicLevels.subList(0, index));
                                t2.add(node);
                                list.add(t2);
                            } else {
                                List<String> t2 = new ArrayList<>(topicLevels);
                                t2.set(index, node);
                                getMatchRetainMessages(t2, index + 1, list);
                            }
                        }
                    });
                }
                break;
            default:
                String count = hash().hget(RedisKey.topicRetainChild(topicLevels.subList(0, index)), level);
                if (count != null && Integer.parseInt(count) > 0) {
                    if (level.equals(Topics.END) && index == topicLevels.size() - 1) {
                        list.add(topicLevels);
                    } else {
                        getMatchRetainMessages(topicLevels, index + 1, list);
                    }
                }
                break;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<InternalMessage<Publish>> getMatchRetainMessages(List<String> topicLevels) {
        List<InternalMessage<Publish>> r = new ArrayList<>();
        if (Topics.isTopicFilter(topicLevels)) {
            List<List<String>> l = new ArrayList<>();
            getMatchRetainMessages(topicLevels, 0, l);
            l.forEach(t -> {
                List<String> ids = list().lrange(RedisKey.topicRetainList(t), 0, -1);
                if (ids != null) {
                    ids.forEach(retainId -> {
                        InternalMessage<Publish> m = mapToInternal(hash().hgetall(RedisKey.topicRemainMessage(t, Integer.parseInt(retainId))));
                        if (m != null) r.add(m);
                    });
                }
            });
        } else {
            List<String> ids = list().lrange(RedisKey.topicRetainList(topicLevels), 0, -1);
            if (ids != null) {
                ids.forEach(retainId -> {
                    InternalMessage<Publish> m = mapToInternal(hash().hgetall(RedisKey.topicRemainMessage(topicLevels, Integer.parseInt(retainId))));
                    if (m != null) r.add(m);
                });
            }
        }

        return r;
    }
}
