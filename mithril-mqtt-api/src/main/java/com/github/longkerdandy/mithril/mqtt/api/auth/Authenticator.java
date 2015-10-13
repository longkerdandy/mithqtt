package com.github.longkerdandy.mithril.mqtt.api.auth;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Authenticator
 */
public interface Authenticator {

    /**
     * Authorize client CONNECT
     *
     * @param clientId Client Id
     * @param userName User Name
     * @param password Password
     * @return Authorize Result
     */
    CompletableFuture<AuthorizeResult> authConnect(String clientId, String userName, String password);

    /**
     * Authorize client PUBLISH
     *
     * @param clientId  Client Id
     * @param userName  User Name
     * @param topicName Topic Name
     * @param qos       QoS
     * @param retain    Retain
     * @return Authorize Result
     */
    CompletableFuture<AuthorizeResult> authPublish(String clientId, String userName, String topicName, int qos, boolean retain);

    /**
     * Authorize client SUBSCRIBE
     *
     * @param clientId   Client Id
     * @param userName   User Name
     * @param topics     List of Topic
     * @param requestQos List of request QoS
     * @return List of granted QoS
     */
    CompletableFuture<List<Integer>> authSubscribe(String clientId, String userName, List<String> topics, List<Integer> requestQos);
}
