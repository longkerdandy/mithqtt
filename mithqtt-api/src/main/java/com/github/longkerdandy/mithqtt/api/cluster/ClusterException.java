package com.github.longkerdandy.mithqtt.api.cluster;

/**
 * Cluster Exception
 */
public class ClusterException extends Exception {

    public ClusterException() {
    }

    public ClusterException(String message) {
        super(message);
    }

    public ClusterException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClusterException(Throwable cause) {
        super(cause);
    }

    public ClusterException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
