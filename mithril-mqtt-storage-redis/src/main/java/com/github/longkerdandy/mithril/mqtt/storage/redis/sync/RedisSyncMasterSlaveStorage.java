package com.github.longkerdandy.mithril.mqtt.storage.redis.sync;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.*;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.redisson.Config;
import org.redisson.Redisson;
import org.redisson.connection.RoundRobinLoadBalancer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Synchronized Storage for Master Slave Redis setup
 */
public class RedisSyncMasterSlaveStorage extends RedisSyncSingleStorage {

    private static final Logger logger = LoggerFactory.getLogger(RedisSyncMasterSlaveStorage.class);

    // A scalable thread-safe Redis client. Multiple threads may share one connection if they avoid
    // blocking and transactional operations such as BLPOP and MULTI/EXEC.
    private RedisClient lettuceMasterSlave;
    // A thread-safe connection to a redis server. Multiple threads may share one StatefulRedisConnection
    private StatefulRedisConnection<String, String> lettuceMasterSlaveConn;

    @SuppressWarnings("unused")
    protected RedisHashCommands<String, String> hash() {
        return this.lettuceMasterSlaveConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisKeyCommands<String, String> key() {
        return this.lettuceMasterSlaveConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisStringCommands<String, String> string() {
        return this.lettuceMasterSlaveConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisListCommands<String, String> list() {
        return this.lettuceMasterSlaveConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisSetCommands<String, String> set() {
        return this.lettuceMasterSlaveConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisSortedSetCommands<String, String> sortedSet() {
        return this.lettuceMasterSlaveConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisScriptingCommands<String, String> script() {
        return this.lettuceMasterSlaveConn.sync();
    }

    @SuppressWarnings("unused")
    protected RedisServerCommands<String, String> server() {
        return this.lettuceMasterSlaveConn.sync();
    }

    @Override
    public void init(AbstractConfiguration config) {
        if (!config.getString("redis.type").equals("master_slave")) {
            logger.error("RedisSyncSingleStorage class can only be used with master slave redis setup, but redis.type value is {}", config.getString("redis.type"));
        }

        List<String> address = parseRedisAddress(config.getString("redis.address"), 6379);
        int databaseNumber = config.getInt("redis.database", 0);
        String password = StringUtils.isNotEmpty(config.getString("redis.password")) ? config.getString("redis.password") + "@" : "";

        // lettuce
        RedisURI lettuceURI = RedisURI.create("redis://" + password + address.get(0) + "/" + databaseNumber);
        this.lettuceMasterSlave = RedisClient.create(lettuceURI);
        this.lettuceMasterSlaveConn = this.lettuceMasterSlave.connect();

        // redisson
        String masterNode = address.get(0);
        String[] slaveNodes = address.subList(1, address.size()).toArray(new String[address.size() - 1]);
        Config redissonConfig = new Config();
        redissonConfig.useMasterSlaveConnection()
                .setMasterAddress(masterNode)
                .setLoadBalancer(new RoundRobinLoadBalancer())
                .addSlaveAddress(slaveNodes)
                .setDatabase(databaseNumber)
                .setPassword(StringUtils.isNotEmpty(password) ? password : null);
        this.redisson = Redisson.create(redissonConfig);
    }

    @Override
    public void destroy() {
        // shutdown this client and close all open connections
        if (this.lettuceMasterSlaveConn != null) this.lettuceMasterSlaveConn.close();
        if (this.lettuceMasterSlave != null) this.lettuceMasterSlave.shutdown();
        if (this.redisson != null) this.redisson.shutdown();
    }
}
