package com.github.longkerdandy.mithril.mqtt.api;

import java.util.concurrent.CompletableFuture;

/**
 * Authenticator
 */
public interface Authenticator {

    /**
     * Authorize client CONNECT
     *
     * @param clientId Client Id
     * @param userName User Name provided in CONNECT
     * @param password Password provided in CONNECT
     * @return Authorize Result
     */
    CompletableFuture<AuthorizeResult> authConnect(String clientId, String userName, String password);
}
