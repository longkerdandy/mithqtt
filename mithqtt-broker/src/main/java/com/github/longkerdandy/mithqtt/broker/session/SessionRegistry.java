package com.github.longkerdandy.mithqtt.broker.session;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MQTT Session Registry for local connections
 */
public class SessionRegistry {

    // Logger
    private static final Logger logger = LoggerFactory.getLogger(SessionRegistry.class);
    // Thread safe HashMap as Repository (Client Id : ChannelHandlerContext)
    private final Map<String, ChannelHandlerContext> repo = new ConcurrentHashMap<>();

    /**
     * Save MQTT session for the client
     *
     * @param clientId Client Id
     * @param session  ChannelHandlerContext as Session
     */
    public void saveSession(String clientId, ChannelHandlerContext session) {
        this.repo.put(clientId, session);
    }

    /**
     * Get MQTT session for the client
     *
     * @param clientId Client Id
     * @return ChannelHandlerContext as Session
     */
    public ChannelHandlerContext getSession(String clientId) {
        return this.repo.get(clientId);
    }

    /**
     * Remove MQTT session for the client
     *
     * @param clientId Client Id
     * @return Removed MQTT Session
     */
    public ChannelHandlerContext removeSession(String clientId) {
        return this.repo.remove(clientId);
    }

    /**
     * Remove MQTT session for the client
     * Only if it is currently mapped to the specified value.
     *
     * @param clientId Client Id
     * @param session  ChannelHandlerContext as Session
     * @return {@code true} if the value was removed
     */
    public boolean removeSession(String clientId, ChannelHandlerContext session) {
        return this.repo.remove(clientId, session);
    }

    /**
     * Send MQTT message to specific client
     *
     * @param msg      MQTT Message to be sent
     * @param clientId Client Id
     * @param packetId Packet Id
     * @param flush    Flush?
     */
    public void sendMessage(MqttMessage msg, String clientId, Integer packetId, boolean flush) {
        ChannelHandlerContext ctx = getSession(clientId);
        if (ctx == null) {
            String pid = packetId == null || packetId <= 0 ? "" : String.valueOf(packetId);
            logger.debug("Message failed: Message {} {} failed to send to {}: Client not connected to this node", msg.fixedHeader().messageType(), pid, clientId);
            return;
        }
        sendMessage(ctx, msg, clientId, packetId, flush);
    }

    /**
     * Send MQTT message to specific session
     *
     * @param ctx      ChannelHandlerContext as Session
     * @param msg      MQTT Message to be sent
     * @param clientId Client Id
     * @param packetId Packet Id
     * @param flush    Flush?
     */
    public void sendMessage(ChannelHandlerContext ctx, MqttMessage msg, String clientId, Integer packetId, boolean flush) {
        String pid = packetId == null || packetId <= 0 ? "" : String.valueOf(packetId);
        ChannelFuture future = flush ? ctx.writeAndFlush(msg) : ctx.write(msg);
        future.addListener(new GenericFutureListener<ChannelFuture>() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    logger.debug("Message succeed: Message {} {} has been sent to client {} successfully", msg.fixedHeader().messageType(), pid, clientId);
                } else {
                    logger.debug("Message failed: Message {} {} failed to send to client {}: ", msg.fixedHeader().messageType(), pid, clientId, future.cause());
                }
            }
        });
    }
}
