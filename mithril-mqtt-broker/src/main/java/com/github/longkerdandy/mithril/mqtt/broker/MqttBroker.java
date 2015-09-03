package com.github.longkerdandy.mithril.mqtt.broker;

import com.github.longkerdandy.mithril.mqtt.api.Authenticator;
import com.github.longkerdandy.mithril.mqtt.api.Communicator;
import com.github.longkerdandy.mithril.mqtt.broker.handler.AsyncRedisHandler;
import com.github.longkerdandy.mithril.mqtt.broker.session.SessionRegistry;
import com.github.longkerdandy.mithril.mqtt.storage.redis.RedisStorage;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * MQTT Bridge
 */
public class MqttBroker {

    public static void main(String[] args) throws Exception {
        // load config
        String s = args.length >= 1 ? args[0] : "config/broker.properties";
        PropertiesConfiguration config = new PropertiesConfiguration(s);

        Authenticator authenticator = null;
        Communicator communicator = null;
        RedisStorage redis = null;

        // tcp server
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
        SessionRegistry registry = new SessionRegistry();
        EventLoopGroup bossGroup = config.getBoolean("netty.useNativeTransport") ? new EpollEventLoopGroup() : new NioEventLoopGroup();
        EventLoopGroup workerGroup = config.getBoolean("netty.useNativeTransport") ? new EpollEventLoopGroup() : new NioEventLoopGroup();
        EventLoopGroup handlerGroup = config.getBoolean("netty.useNativeTransport") ? new EpollEventLoopGroup() : new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(config.getBoolean("netty.useNativeTransport") ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            // mqtt encoder & decoder
                            p.addLast(new MqttEncoder());
                            p.addLast(new MqttDecoder());
                            // handler
                            p.addLast(handlerGroup, new AsyncRedisHandler(authenticator, communicator, redis, registry, config));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, config.getInt("netty.soBacklog"))
                    .childOption(ChannelOption.SO_KEEPALIVE, config.getBoolean("netty.soKeepAlive"));

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(config.getString("mqtt.host"), config.getInt("mqtt.port")).sync();

            // Wait until the server socket is closed.
            // Do this to gracefully shut down the server.
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}
