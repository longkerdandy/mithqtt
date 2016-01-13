package com.github.longkerdandy.mithqtt.storage.redis;

import java.util.List;

/**
 * Key Definition
 */
public class RedisKey {

    // Set of server's connected clients
    // Value - Client Id
    public static String connectedClients(String node) {
        return "server:" + node + ":client";
    }

    // Key indicates client's connected node
    public static String connectedNode(String clientId) {
        return "client:" + clientId + ":node";
    }

    // Key indicates client session state
    public static String session(String clientId) {
        return "client:" + clientId + ":session";
    }

    // Key indicates next packet id for the client
    public static String nextPacketId(String clientId) {
        return "client:" + clientId + ":pid";
    }

    // Set of inbound QoS 2 message's packet id from the client
    // Value - MQTT Message's Packet Id
    public static String qos2Set(String clientId) {
        return "client:" + clientId + ":qos2";
    }

    // List of outbound in-flight messages' packet id for the client
    // Value - MQTT Message's Packet Id in order
    public static String inFlightList(String clientId) {
        return "client:" + clientId + ":in.flight";
    }

    // Hash of outbound in-flight message for the client
    // MQTT Message in Hash
    public static String inFlightMessage(String clientId, int packetId) {
        return "client:" + clientId + ":in.flight:" + packetId;
    }

    // Hash of client's subscriptions
    // Key - Topic Name or Topic Filter
    // Value - Qos Level
    public static String subscription(String clientId) {
        return "client:" + clientId + ":subscription";
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
    //    public static String topicName(String topicName) {
    //        return "topic:n:" + topicName;
    //    }

    // Hash of topic filter's subscriptions
    // Key - Client Id (which subscribed to this topic filter)
    // Value - QoS Level
    public static String topicFilter(List<String> topicLevels) {
        return "topic:f:" + String.join("/", topicLevels);
    }

    // Hash of topic filter's subscriptions
    // Key - Client Id (which subscribed to this topic filter)
    // Value - QoS Level
    //    public static String topicFilter(String topicFilter) {
    //        return "topic:f:" + topicFilter;
    //    }

    // Hash of topic filter's children in trie tree
    // Key - Topic Level (child node in the topic filter tree)
    // Value - Count (how many subscriptions traverse this node, 0 means route not exist)
    public static String topicFilterChild(List<String> topicLevels) {
        return topicLevels == null || topicLevels.isEmpty() ? "{topic:f:tree}" : "{topic:f:tree}:" + String.join("/", topicLevels);
    }

    // Key indicates next retain id for the topic name
    public static String nextRetainId(List<String> topicLevels) {
        return "topic:r:" + String.join("/", topicLevels) + ":rid";
    }

    // List of remain message's retain id for the topic name
    // Value MQTT Message's Packet Id
    public static String topicRetainList(List<String> topicLevels) {
        return "topic:r:" + String.join("/", topicLevels);
    }

    // Hash of retain message for the topic name
    // MQTT Message in Hash
    public static String topicRemainMessage(List<String> topicLevels, int retainId) {
        return "topic:r:" + String.join("/", topicLevels) + ":" + retainId;
    }

    // Hash of topic retain's children in trie tree
    // Key - Topic Level (child node in the topic retain tree)
    // Value - Count (how many subscriptions traverse this node, 0 means route not exist)
    public static String topicRetainChild(List<String> topicLevels) {
        return topicLevels == null || topicLevels.isEmpty() ? "{topic:r:tree}" : "{topic:r:tree}:" + String.join("/", topicLevels);
    }
}
