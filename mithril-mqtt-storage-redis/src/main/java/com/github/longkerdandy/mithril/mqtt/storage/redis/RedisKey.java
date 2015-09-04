package com.github.longkerdandy.mithril.mqtt.storage.redis;

import org.apache.commons.lang3.BooleanUtils;

/**
 * Key Definition
 */
public class RedisKey {

    // Set client's connected nodes
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

    // Hash for client's in-flight message
    public static String inFlightMessage(String clientId, int packetId) {
        return "client:" + clientId + ":in.flight:" + packetId;
    }
}
