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
import org.redisson.connection.balancer.RoundRobinLoadBalancer;

import java.util.List;

/**
 * Synchronized Storage for Master Slave Redis setup
 */
@SuppressWarnings("unused")
public class RedisSyncMasterSlaveStorage extends RedisSyncSingleStorage {

    // A scalable thread-safe Redis client. Multiple threads may share one connection if they avoid
    // blocking and transactional operations such as BLPOP and MULTI/EXEC.
    private RedisClient lettuceMasterSlave;
    // A thread-safe connection to a redis server. Multiple threads may share one StatefulRedisConnection
    private StatefulRedisMasterSlaveConnection<String, String> lettuceMasterSlaveConn;

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
            throw new IllegalStateException("RedisSyncSingleStorage class can only be used with master slave redis setup, but redis.type value is " + config.getString("redis.type"));
        }

        List<String> address = parseRedisAddress(config.getString("redis.address"), 6379);
        int databaseNumber = config.getInt("redis.database", 0);
        String password = StringUtils.isNotEmpty(config.getString("redis.password")) ? config.getString("redis.password") + "@" : "";

        // lettuce
        RedisURI lettuceURI = RedisURI.create("redis://" + password + address.get(0) + "/" + databaseNumber);
        this.lettuceMasterSlave = RedisClient.create(lettuceURI);
        this.lettuceMasterSlaveConn = MasterSlave.connect(this.lettuceMasterSlave, new Utf8StringCodec(), lettuceURI);
        this.lettuceMasterSlaveConn.setReadFrom(ReadFrom.valueOf(config.getString("redis.read")));

        // redisson
        String masterNode = address.get(0);
        String[] slaveNodes = address.subList(1, address.size()).toArray(new String[address.size() - 1]);
        Config redissonConfig = new Config();
        redissonConfig.useMasterSlaveServers()
                .setMasterAddress(masterNode)
                .setLoadBalancer(new RoundRobinLoadBalancer())
                .addSlaveAddress(slaveNodes)
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
        if (this.lettuceMasterSlaveConn != null) this.lettuceMasterSlaveConn.close();
        if (this.lettuceMasterSlave != null) this.lettuceMasterSlave.shutdown();
        if (this.redisson != null) this.redisson.shutdown();
    }
}
