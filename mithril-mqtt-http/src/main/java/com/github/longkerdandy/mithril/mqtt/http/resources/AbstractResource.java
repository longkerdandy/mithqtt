package com.github.longkerdandy.mithril.mqtt.http.resources;

import com.github.longkerdandy.mithril.mqtt.api.auth.Authenticator;
import com.github.longkerdandy.mithril.mqtt.api.comm.HttpCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.metrics.MetricsService;
import com.github.longkerdandy.mithril.mqtt.storage.redis.sync.RedisSyncStorage;

/**
 * Abstract Base Resource
 */
public abstract class AbstractResource {

    protected final String serverId;
    protected final RedisSyncStorage redis;
    protected final HttpCommunicator communicator;
    protected final Authenticator authenticator;
    protected final MetricsService metrics;

    public AbstractResource(String serverId, RedisSyncStorage redis, HttpCommunicator communicator, Authenticator authenticator, MetricsService metrics) {
        this.serverId = serverId;
        this.redis = redis;
        this.communicator = communicator;
        this.authenticator = authenticator;
        this.metrics = metrics;
    }
}
