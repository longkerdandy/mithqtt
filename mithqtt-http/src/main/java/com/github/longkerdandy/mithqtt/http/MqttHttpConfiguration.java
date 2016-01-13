package com.github.longkerdandy.mithqtt.http;

import io.dropwizard.Configuration;

import javax.validation.constraints.NotNull;
import java.util.regex.Pattern;

/**
 * MqttHttp Configuration
 */
@SuppressWarnings("unused")
public class MqttHttpConfiguration extends Configuration {

    @NotNull
    private String serverId;
    private String clientIdValidator;
    private String topicNameValidator;
    private String topicFilterValidator;

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public String getClientIdValidator() {
        return clientIdValidator;
    }

    public void setClientIdValidator(String clientIdValidator) {
        this.clientIdValidator = clientIdValidator;
    }

    public String getTopicNameValidator() {
        return topicNameValidator;
    }

    public void setTopicNameValidator(String topicNameValidator) {
        this.topicNameValidator = topicNameValidator;
    }

    public String getTopicFilterValidator() {
        return topicFilterValidator;
    }

    public void setTopicFilterValidator(String topicFilterValidator) {
        this.topicFilterValidator = topicFilterValidator;
    }
}
