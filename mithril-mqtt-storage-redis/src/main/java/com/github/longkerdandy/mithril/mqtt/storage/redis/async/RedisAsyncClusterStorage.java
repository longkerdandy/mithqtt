package com.github.longkerdandy.mithril.mqtt.storage.redis.async;

import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.async.*;
import com.lambdaworks.redis.cluster.ClusterClientOptions;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;

import java.util.concurrent.TimeUnit;

/**
 * Asynchronous Storage for Cluster Redis setup
 * Redis 3.0 above
 */
public class RedisAsyncClusterStorage extends RedisAsyncPlainStorage {

    // A scalable thread-safe Redis cluster client. Multiple threads may share one connection. The
    // cluster client handles command routing based on the first key of the command and maintains a view on the cluster that is
    // available when calling the {@link #getPartitions()} method.
    private RedisClusterClient clusterClient;
    // A stateful cluster connection providing. Advanced cluster connections provide transparent command routing based on the first
    // command key.
    private StatefulRedisClusterConnection<String, String> clusterConn;

    @SuppressWarnings("unused")
    protected RedisHashAsyncCommands<String, String> hash() {
        return this.clusterConn.async();
    }

    @SuppressWarnings("unused")
    protected RedisKeyAsyncCommands<String, String> key() {
        return this.clusterConn.async();
    }

    @SuppressWarnings("unused")
    protected RedisStringAsyncCommands<String, String> string() {
        return this.clusterConn.async();
    }

    @SuppressWarnings("unused")
    protected RedisListAsyncCommands<String, String> list() {
        return this.clusterConn.async();
    }

    @SuppressWarnings("unused")
    protected RedisSetAsyncCommands<String, String> set() {
        return this.clusterConn.async();
    }

    @SuppressWarnings("unused")
    protected RedisSortedSetAsyncCommands<String, String> sortedSet() {
        return this.clusterConn.async();
    }

    @SuppressWarnings("unused")
    protected RedisScriptingAsyncCommands<String, String> script() {
        return this.clusterConn.async();
    }

    @SuppressWarnings("unused")
    protected RedisServerAsyncCommands<String, String> server() {
        return this.clusterConn.async();
    }

    @Override
    public void init(RedisURI redisURI) {
        // Create a new client that connects to the supplied {@link RedisURI uri}. You can connect to different Redis servers but
        // you must supply a {@link RedisURI} on connecting.
        this.clusterClient = RedisClusterClient.create(redisURI);
        // Enabling regular cluster topology view updates
        this.clusterClient.setOptions(new ClusterClientOptions.Builder()
                .refreshClusterView(true)
                .refreshPeriod(1, TimeUnit.MINUTES)
                .build());
        this.clusterConn = this.clusterClient.connect();
    }

    @Override
    public void destroy() {
        // shutdown this client and close all open connections
        if (this.clusterConn != null) this.clusterConn.close();
        if (this.clusterClient != null) this.clusterClient.shutdown();
    }
}
