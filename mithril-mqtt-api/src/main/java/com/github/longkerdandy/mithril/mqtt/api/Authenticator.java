package com.github.longkerdandy.mithril.mqtt.api;

import java.util.concurrent.CompletableFuture;

/**
 * Authenticator
 */
public interface Authenticator {

    int AUTH_SUCCESS = 200;

    /**
     * Authorize client CONNECT
     *
     * @param clientId Client Id
     * @param userName User Name provided in CONNECT
     * @param password Password provided in CONNECT
     * @return Result
     */
    CompletableFuture<Integer> authConnect(String clientId, String userName, String password);
}
