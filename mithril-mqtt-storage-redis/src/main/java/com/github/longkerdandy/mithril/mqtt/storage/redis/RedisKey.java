package com.github.longkerdandy.mithril.mqtt.storage.redis;

/**
 * Key Definition
 */
public class RedisKey {

    // List of client's in-flight messages' packet id
    public static String inFlightList(String clientId) {
        return "client:" + clientId + ":in.flight";
    }

    // Hash for client's in-flight message
    public static String inFlightMessage(String clientId, int packetId) {
        return "client:" + clientId + ":in.flight:" + packetId;
    }
}
