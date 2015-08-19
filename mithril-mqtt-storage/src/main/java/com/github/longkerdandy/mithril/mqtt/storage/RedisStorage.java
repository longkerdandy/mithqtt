package com.github.longkerdandy.mithril.mqtt.storage;

import com.lambdaworks.redis.*;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.RedisCommands;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis Storage
 */
public class RedisStorage {

    protected RedisClient client;
    protected StatefulRedisConnection<String, String> conn;

    public RedisStorage(String uri) {
        this.client = new RedisClient(RedisURI.create(uri));
    }

    public void init() {
        this.conn = this.client.connect();
    }

    public void destory() {
        if (this.conn != null) this.conn.close();
        this.client.shutdown();
    }

    public void createSubscription(String clientId, String topicFilter) {
        RedisCommands<String, String> commands = this.conn.sync();

    }

    public void removeSubscription(String clientId, String topicFilter) {

    }

    public List<String> getSubscriber(String topic) {
        List<String> l = new ArrayList<>();
        return l;
    }
}
