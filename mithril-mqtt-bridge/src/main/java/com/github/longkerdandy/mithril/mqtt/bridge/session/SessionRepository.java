package com.github.longkerdandy.mithril.mqtt.bridge.session;

import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MQTT Session Local Repository in Memory
 */
public class SessionRepository {

    // Logger
    private static final Logger logger = LoggerFactory.getLogger(SessionRepository.class);
    // Thread safe HashMap as Repository (Client Id : ChannelHandlerContext)
    private final Map<String, ChannelHandlerContext> repo = new ConcurrentHashMap<>();

    /**
     * Save MQTT session
     *
     * @param clientId Client Id
     * @param session  ChannelHandlerContext as Session
     */
    public void saveSession(String clientId, ChannelHandlerContext session) {
        this.repo.put(clientId, session);
    }

    /**
     * Get MQTT session
     *
     * @param clientId Client Id
     * @return ChannelHandlerContext as Session
     */
    public ChannelHandlerContext getSession(String clientId) {
        return this.repo.get(clientId);
    }

    /**
     * Remove MQTT session
     * Only if it is currently mapped to the specified value.
     *
     * @param clientId Client Id
     * @param session  ChannelHandlerContext as Session
     * @return {@code true} if the value was removed
     */
    public boolean removeSession(String clientId, ChannelHandlerContext session) {
        return this.repo.remove(clientId, session);
    }
}
