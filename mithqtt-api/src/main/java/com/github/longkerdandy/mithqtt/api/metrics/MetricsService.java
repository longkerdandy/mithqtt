package com.github.longkerdandy.mithqtt.api.metrics;

import io.netty.handler.codec.mqtt.MqttMessageType;
import org.apache.commons.configuration.AbstractConfiguration;

/**
 * Metrics Service
 */
@SuppressWarnings("unused")
public interface MetricsService {

    /**
     * Init the metrics service
     *
     * @param config Metrics Service Configuration
     */
    void init(AbstractConfiguration config);

    /**
     * Destroy the metrics service
     */
    void destroy();

    /**
     * Received a message from the client on the broker
     *
     * @param clientId  Client Id
     * @param brokerId  Broker Id
     * @param direction Message Direction
     * @param type      MQTT Message Type
     */
    void measurement(String clientId, String brokerId, MessageDirection direction, MqttMessageType type);

    /**
     * Received a message on the broker
     *
     * @param brokerId  Broker Id
     * @param direction Message Direction
     * @param type      MQTT Message Type
     */
    void measurement(String brokerId, MessageDirection direction, MqttMessageType type);


    /**
     * Received a message on the broker
     *
     * @param brokerId  Broker Id
     * @param direction Message Direction
     * @param length    MQTT Message Length in bytes
     */
    void measurement(String brokerId, MessageDirection direction, long length);
}
