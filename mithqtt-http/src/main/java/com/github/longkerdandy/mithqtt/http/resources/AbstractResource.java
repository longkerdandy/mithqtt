package com.github.longkerdandy.mithqtt.http.resources;

import com.github.longkerdandy.mithqtt.http.util.Validator;
import com.github.longkerdandy.mithqtt.api.auth.Authenticator;
import com.github.longkerdandy.mithqtt.api.comm.HttpCommunicator;
import com.github.longkerdandy.mithqtt.api.metrics.MetricsService;
import com.github.longkerdandy.mithqtt.storage.redis.sync.RedisSyncStorage;

/**
 * Abstract Base Resource
 */
public abstract class AbstractResource {

    protected final String serverId;
    protected final Validator validator;
    protected final RedisSyncStorage redis;
    protected final HttpCommunicator communicator;
    protected final Authenticator authenticator;
    protected final MetricsService metrics;

    public AbstractResource(String serverId, Validator validator, RedisSyncStorage redis, HttpCommunicator communicator, Authenticator authenticator, MetricsService metrics) {
        this.serverId = serverId;
        this.validator = validator;
        this.redis = redis;
        this.communicator = communicator;
        this.authenticator = authenticator;
        this.metrics = metrics;
    }
}
