package com.github.longkerdandy.mithqtt.http.util;

import com.github.longkerdandy.mithqtt.http.MqttHttpConfiguration;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Pattern;

/**
 * Validator
 */
@SuppressWarnings("unused")
public class Validator {

    // MQTT client id validate regex pattern
    protected Pattern clientIdPattern;
    // MQTT topic name validate regex pattern
    protected Pattern topicNamePattern;
    // MQTT topic filter validate regex pattern
    protected Pattern topicFilterPattern;

    public Validator(MqttHttpConfiguration config) {
        if (StringUtils.isNotBlank(config.getClientIdValidator()))
            this.clientIdPattern = Pattern.compile(config.getClientIdValidator());
        if (StringUtils.isNotBlank(config.getTopicNameValidator()))
            this.topicNamePattern = Pattern.compile(config.getTopicNameValidator());
        if (StringUtils.isNotBlank(config.getTopicFilterValidator()))
            this.topicFilterPattern = Pattern.compile(config.getTopicFilterValidator());
    }

    /**
     * Is MQTT topic name (no wildcards) valid
     *
     * @param topicName Topic Name
     * @return True if valid
     */
    public boolean isTopicNameValid(String topicName) {
        return !StringUtils.isEmpty(topicName) &&
                !topicName.contains("+") &&
                !topicName.contains("#") &&
                (this.topicNamePattern == null || this.topicNamePattern.matcher(topicName).matches());
    }

    /**
     * Is MQTT topic filter (may contain wildcards) valid
     *
     * @param topicFilter Topic Filter
     * @return True if valid
     */
    public boolean isTopicFilterValid(String topicFilter) {
        return !StringUtils.isEmpty(topicFilter) &&
                (this.topicFilterPattern == null || this.topicFilterPattern.matcher(topicFilter).matches());
    }

    /**
     * Is MQTT client id valid
     *
     * @param clientId Client Id
     * @return True if valid
     */
    public boolean isClientIdValid(String clientId) {
        return this.clientIdPattern == null || this.clientIdPattern.matcher(clientId).matches();
    }
}
