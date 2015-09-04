package com.github.longkerdandy.mithril.mqtt.storage.redis;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

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

    // Hash of wildcard topic 's subscriptions
    public static String topicWildcard(String topicNode) {
        return "topic:w:" + StringUtils.removeEnd(topicNode, "/");
    }

    // Set of wildcard topic node's children
    public static String topicWildcardChild(String topicNode) {
        return "topic:w:" + StringUtils.removeEnd(topicNode, "/") + ":child";
    }
}
