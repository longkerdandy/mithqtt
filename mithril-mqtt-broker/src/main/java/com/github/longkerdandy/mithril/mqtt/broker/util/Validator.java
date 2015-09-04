package com.github.longkerdandy.mithril.mqtt.broker.util;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Pattern;

/**
 * Validator
 */
public class Validator {

    // MQTT client id validate regex pattern
    protected Pattern clientIdPattern;

    public Validator(PropertiesConfiguration config) {
        if (StringUtils.isNotBlank(config.getString("mqtt.clientId.validator")))
            this.clientIdPattern = Pattern.compile(config.getString("mqtt.clientId.validator"));
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
