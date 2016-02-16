package com.github.longkerdandy.mithqtt.storage.redis.sync;

import com.lambdaworks.redis.ReadFrom;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.sync.*;
import com.lambdaworks.redis.codec.Utf8StringCodec;
import com.lambdaworks.redis.masterslave.MasterSlave;
import com.lambdaworks.redis.masterslave.StatefulRedisMasterSlaveConnection;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.redisson.Config;
import org.redisson.ReadMode;
import org.redisson.Redisson;

import java.util.List;

/**
 * Synchronized Storage for Master Slave Redis setup
 */
@SuppressWarnings("unused")
public class RedisSyncSentinelStorage extends RedisSyncSingleStorage {

    // A scalable thread-safe Redis client. Multiple threads may share one connection if they avoid
    // blocking and transactional operations such as BLPOP and MULTI/EXEC.
    private RedisClient lettuceSentinel;
    // A thread-safe connection to a redis server. Multiple threads may share one StatefulRedisConnection
    private StatefulRedisMasterSlaveConnection<String, String> lettuceSentinelConn;
    // Main infrastructure class allows to get access to all Redisson objects on top of Redis server

    @SuppressWarnings("unused")
    protected RedisHashCommands<String, String> hash() {
        return this.lettuceSentinelConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisKeyCommands<String, String> key() {
        return this.lettuceSentinelConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisStringCommands<String, String> string() {
        return this.lettuceSentinelConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisListCommands<String, String> list() {
        return this.lettuceSentinelConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisSetCommands<String, String> set() {
        return this.lettuceSentinelConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisSortedSetCommands<String, String> sortedSet() {
        return this.lettuceSentinelConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisScriptingCommands<String, String> script() {
        return this.lettuceSentinelConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisServerCommands<String, String> server() {
        return this.lettuceSentinelConn.sync();
    }

    @Override
    public void init(AbstractConfiguration config) {
        if (!config.getString("redis.type").equals("sentinel")) {
            throw new IllegalStateException("RedisSyncSingleStorage class can only be used with sentinel redis setup, but redis.type value is " + config.getString("redis.type"));
        }

        List<String> address = parseRedisAddress(config.getString("redis.address"), 26379);
        int databaseNumber = config.getInt("redis.database", 0);
        String password = StringUtils.isNotEmpty(config.getString("redis.password")) ? config.getString("redis.password") + "@" : "";
        String masterId = config.getString("redis.master");

        // lettuce
        RedisURI lettuceURI = RedisURI.create("redis-sentinel://" + password + String.join(",", address) + "/" + databaseNumber + "#" + masterId);
        this.lettuceSentinel = RedisClient.create(lettuceURI);
        this.lettuceSentinelConn = MasterSlave.connect(this.lettuceSentinel, new Utf8StringCodec(), lettuceURI);
        this.lettuceSentinelConn.setReadFrom(ReadFrom.valueOf(config.getString("redis.read")));

        // redisson
        Config redissonConfig = new Config();
        redissonConfig.useSentinelServers()
                .setMasterName(masterId)
                .addSentinelAddress(address.toArray(new String[address.size()]))
                .setReadMode(ReadMode.MASTER)
                .setDatabase(databaseNumber)
                .setPassword(StringUtils.isNotEmpty(password) ? password : null);
        this.redisson = Redisson.create(redissonConfig);

        // params
        initParams(config);
    }

    @Override
    public void destroy() {
        // shutdown this client and close all open connections
        if (this.lettuceSentinelConn != null) this.lettuceSentinelConn.close();
        if (this.lettuceSentinel != null) this.lettuceSentinel.shutdown();
        if (this.redisson != null) this.redisson.shutdown();
    }
}
