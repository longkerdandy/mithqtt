package com.github.longkerdandy.mithril.mqtt.authenticator.dummy;

import com.github.longkerdandy.mithril.mqtt.api.auth.Authenticator;
import com.github.longkerdandy.mithril.mqtt.api.auth.AuthorizeResult;
import io.netty.handler.codec.mqtt.MqttSubAckReturnCode;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Dummy Authenticator
 * Which simply authorized everything
 */
public class DummyAuthenticator implements Authenticator {

    @Override
    public void init(PropertiesConfiguration config) {
    }

    @Override
    public void destroy() {
    }

    @Override
    public CompletableFuture<AuthorizeResult> authConnectAsync(String clientId, String userName, String password) {
        return CompletableFuture.completedFuture(AuthorizeResult.OK);
    }

    @Override
    public CompletableFuture<AuthorizeResult> authPublishAsync(String clientId, String userName, String topicName, int qos, boolean retain) {
        return CompletableFuture.completedFuture(AuthorizeResult.OK);
    }

    @Override
    public CompletableFuture<List<MqttSubAckReturnCode>> authSubscribeAsync(String clientId, String userName, List<MqttTopicSubscription> requestSubscriptions) {
        List<MqttSubAckReturnCode> r = new ArrayList<>();
        requestSubscriptions.forEach(subscription ->
                r.add(MqttSubAckReturnCode.valueOf(subscription.requestedQos().value())));
        return CompletableFuture.completedFuture(r);
    }
}
