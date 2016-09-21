package com.github.longkerdandy.mithqtt.http.resources;

import com.github.longkerdandy.mithqtt.api.auth.Authenticator;
import com.github.longkerdandy.mithqtt.api.cluster.Cluster;
import com.github.longkerdandy.mithqtt.api.storage.sync.SyncStorage;
import com.github.longkerdandy.mithqtt.http.util.Validator;

/**
 * Abstract Base Resource
 */
public abstract class AbstractResource {

    protected final String serverId;
    protected final Validator validator;
    protected final SyncStorage storage;
    protected final Cluster cluster;
    protected final Authenticator authenticator;

    public AbstractResource(String serverId, Validator validator, SyncStorage storage, Cluster cluster, Authenticator authenticator) {
        this.serverId = serverId;
        this.validator = validator;
        this.storage = storage;
        this.cluster = cluster;
        this.authenticator = authenticator;
    }
}
