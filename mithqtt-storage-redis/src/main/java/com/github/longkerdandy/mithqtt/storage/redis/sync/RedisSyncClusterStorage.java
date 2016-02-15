package com.github.longkerdandy.mithqtt.storage.redis.sync;

import com.lambdaworks.redis.ReadFrom;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.sync.*;
import com.lambdaworks.redis.cluster.ClusterClientOptions;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.redisson.Config;
import org.redisson.ReadMode;
import org.redisson.Redisson;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Synchronized Storage for Cluster Redis setup
 */
@SuppressWarnings("unused")
public class RedisSyncClusterStorage extends RedisSyncSingleStorage {

    // A scalable thread-safe Redis cluster client. Multiple threads may share one connection. The
    // cluster client handles command routing based on the first key of the command and maintains a view on the cluster that is
    // available when calling the {@link #getPartitions()} method.
    private RedisClusterClient lettuceCluster;
    // A stateful cluster connection providing. Advanced cluster connections provide transparent command routing based on the first
    // command key.
    private StatefulRedisClusterConnection<String, String> lettuceClusterConn;

    @SuppressWarnings("unused")
    protected RedisHashCommands<String, String> hash() {
        return this.lettuceClusterConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisKeyCommands<String, String> key() {
        return this.lettuceClusterConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisStringCommands<String, String> string() {
        return this.lettuceClusterConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisListCommands<String, String> list() {
        return this.lettuceClusterConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisSetCommands<String, String> set() {
        return this.lettuceClusterConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisSortedSetCommands<String, String> sortedSet() {
        return this.lettuceClusterConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisScriptingCommands<String, String> script() {
        return this.lettuceClusterConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisServerCommands<String, String> server() {
        return this.lettuceClusterConn.sync();
    }

    @Override
    public void init(AbstractConfiguration config) {
        if (!config.getString("redis.type").equals("cluster")) {
            throw new IllegalStateException("RedisSyncSingleStorage class can only be used with cluster redis setup, but redis.type value is " + config.getString("redis.type"));
        }

        List<String> address = parseRedisAddress(config.getString("redis.address"), 6379);
        int databaseNumber = config.getInt("redis.database", 0);
        String password = StringUtils.isNotEmpty(config.getString("redis.password")) ? config.getString("redis.password") + "@" : "";

        // lettuce
        RedisURI lettuceURI = RedisURI.create("redis://" + password + address.get(0) + "/" + databaseNumber);
        this.lettuceCluster = RedisClusterClient.create(lettuceURI);
        this.lettuceCluster.setOptions(new ClusterClientOptions.Builder()
                .refreshClusterView(true)
                .refreshPeriod(1, TimeUnit.MINUTES)
                .build());
        this.lettuceClusterConn = this.lettuceCluster.connect();
        this.lettuceClusterConn.setReadFrom(ReadFrom.valueOf(config.getString("redis.read")));

        // redisson
        Config redissonConfig = new Config();
        redissonConfig.useClusterServers()
                .setScanInterval(60000)
                .addNodeAddress(address.toArray(new String[address.size()]))
                .setReadMode(ReadMode.MASTER)
                .setPassword(StringUtils.isNotEmpty(password) ? password : null);
        this.redisson = Redisson.create(redissonConfig);

        // params
        initParams(config);
    }

    @Override
    public void destroy() {
        // shutdown this client and close all open connections
        if (this.lettuceClusterConn != null) this.lettuceClusterConn.close();
        if (this.lettuceCluster != null) this.lettuceCluster.shutdown();
        if (this.redisson != null) this.redisson.shutdown();
    }
}
