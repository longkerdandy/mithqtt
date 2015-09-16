package com.github.longkerdandy.mithril.mqtt.api;

import io.netty.handler.codec.mqtt.MqttMessage;

import java.util.Map;
import java.util.Set;

/**
 * Communicator
 */
public interface Communicator {

    /**
     * One to One communication between server nodes
     *
     * @param node     Target Node
     * @param msg      Mqtt Message
     * @param metadata Metadata
     */
    void oneToOne(String node, MqttMessage msg, Map<String, Object> metadata);

    /**
     * One to Many communication between server nodes
     *
     * @param nodes    Target Nodes
     * @param msg      Mqtt Message
     * @param metadata Metadata
     */
    void oneToMany(Set<String> nodes, MqttMessage msg, Map<String, Object> metadata);
}
