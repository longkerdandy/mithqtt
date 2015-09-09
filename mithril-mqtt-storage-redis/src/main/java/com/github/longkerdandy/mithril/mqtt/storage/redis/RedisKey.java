package com.github.longkerdandy.mithril.mqtt.storage.redis;

import org.apache.commons.lang3.BooleanUtils;

import java.util.List;

/**
 * Key Definition
 */
public class RedisKey {

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

    // Hash of topic name's subscriptions
    public static String topicName(List<String> topicLevels) {
        return "topic:n:" + String.join("/", topicLevels);
    }

    // Hash of topic filter's subscriptions
    public static String topicFilter(List<String> topicLevels) {
        return "topic:f:" + String.join("/", topicLevels);
    }

    // Hash of topic filter's children
    public static String topicFilterChild(List<String> topicLevels) {
        return topicLevels == null || topicLevels.isEmpty() ? "topic:tree:" : "topic:tree:" + String.join("/", topicLevels);
    }
}
