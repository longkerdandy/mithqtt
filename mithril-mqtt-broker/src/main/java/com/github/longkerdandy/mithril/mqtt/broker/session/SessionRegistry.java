package com.github.longkerdandy.mithril.mqtt.broker.session;

import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MQTT Session Local Repository in Memory
 */
public class SessionRegistry {

    // Logger
    private static final Logger logger = LoggerFactory.getLogger(SessionRegistry.class);
    // Thread safe HashMap as Repository (Client Id : ChannelHandlerContext)
    private final Map<String, ChannelHandlerContext> repo = new ConcurrentHashMap<>();

    /**
     * Save MQTT com.github.longkerdandy.mithril.mqtt.broker.session
     *
     * @param clientId Client Id
     * @param session  ChannelHandlerContext as Session
     */
    public void saveSession(String clientId, ChannelHandlerContext session) {
        this.repo.put(clientId, session);
    }

    /**
     * Get MQTT com.github.longkerdandy.mithril.mqtt.broker.session
     *
     * @param clientId Client Id
     * @return ChannelHandlerContext as Session
     */
    public ChannelHandlerContext getSession(String clientId) {
        return this.repo.get(clientId);
    }

    /**
     * Remove MQTT com.github.longkerdandy.mithril.mqtt.broker.session
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
