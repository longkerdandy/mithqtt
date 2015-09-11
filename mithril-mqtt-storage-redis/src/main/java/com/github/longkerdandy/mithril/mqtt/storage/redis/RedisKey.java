package com.github.longkerdandy.mithril.mqtt.storage.redis;

import org.apache.commons.lang3.BooleanUtils;

import java.util.List;

/**
 * Key Definition
 */
public class RedisKey {

    // Set of server's connected clients
    public static String connectedClients(String node) {
        return "server:" + node + ":client";
    }

    // Set of client's connected nodes
    public static String connectedNodes(String clientId) {
        return "client:" + clientId + ":node";
    }

    // Key indicates client existence
    public static String clientExist(String clientId) {
        return "client:" + clientId + ":exist";
    }

    // List of client's in-flight messages' packet id
    public static String inFlightList(String clientId, boolean cleanSession) {
        return "client:" + clientId + ":" + BooleanUtils.toString(cleanSession, "1", "0") + ":" + ":in.flight";
    }

    // Hash of client's in-flight message
    public static String inFlightMessage(String clientId, int packetId) {
        return "client:" + clientId + ":in.flight:" + packetId;
    }

    // Hash of client's subscriptions
    // Key - Topic Name or Topic Filter
    // Value - Qos Level
    public static String subscription(String clientId, boolean cleanSession) {
        return "client:" + clientId + ":" + BooleanUtils.toString(cleanSession, "1", "0") + ":subscription";
    }

    // Hash of topic name's subscriptions
    // Key - Client Id (which subscribed to this topic name)
    // Value - QoS Level
    public static String topicName(List<String> topicLevels) {
        return "topic:n:" + String.join("/", topicLevels);
    }
    // Hash of topic name's subscriptions
    // Key - Client Id (which subscribed to this topic name)
    // Value - QoS Level
    public static String topicName(String topicName) {
        return "topic:n:" + topicName;
    }

    // Hash of topic filter's subscriptions
    // Key - Client Id (which subscribed to this topic filter)
    // Value - QoS Level
    public static String topicFilter(List<String> topicLevels) {
        return "topic:f:" + String.join("/", topicLevels);
    }

    // Hash of topic filter's subscriptions
    // Key - Client Id (which subscribed to this topic filter)
    // Value - QoS Level
    public static String topicFilter(String topicFilter) {
        return "topic:f:" + topicFilter;
    }

    // Hash of topic filter's children
    // Key - Topic Level (child node in the topic filter tree)
    // Value - Count (how many subscriptions traverse this node, 0 means route not exist)
    public static String topicFilterChild(List<String> topicLevels) {
        return topicLevels == null || topicLevels.isEmpty() ? "topic:tree:" : "topic:tree:" + String.join("/", topicLevels);
    }
}
