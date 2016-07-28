package com.github.longkerdandy.mithqtt.broker;

import com.github.longkerdandy.mithqtt.api.auth.Authenticator;
import com.github.longkerdandy.mithqtt.broker.cluster.NATSCluster;
import com.github.longkerdandy.mithqtt.broker.handler.SyncRedisHandler;
import com.github.longkerdandy.mithqtt.broker.session.SessionRegistry;
import com.github.longkerdandy.mithqtt.broker.util.Validator;
import com.github.longkerdandy.mithqtt.storage.redis.sync.RedisSyncStorage;
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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * MQTT Bridge
 */
public class MqttBroker {

    private static final Logger logger = LoggerFactory.getLogger(MqttBroker.class);

    public static void main(String[] args) throws Exception {

        logger.debug("Starting MQTT broker ...");

        // load config
        logger.debug("Loading MQTT broker config files ...");
        PropertiesConfiguration brokerConfig;
        PropertiesConfiguration redisConfig;
        PropertiesConfiguration clusterConfig;
        PropertiesConfiguration authenticatorConfig;
        if (args.length >= 4) {
            brokerConfig = new PropertiesConfiguration(args[0]);
            redisConfig = new PropertiesConfiguration(args[1]);
            clusterConfig = new PropertiesConfiguration(args[2]);
            authenticatorConfig = new PropertiesConfiguration(args[3]);
        } else {
            brokerConfig = new PropertiesConfiguration("config/broker.properties");
            redisConfig = new PropertiesConfiguration("config/redis.properties");
            clusterConfig = new PropertiesConfiguration("config/cluster.properties");
            authenticatorConfig = new PropertiesConfiguration("config/authenticator.properties");
        }

        final String brokerId = brokerConfig.getString("broker.id");

        // validator
        logger.debug("Initializing validator ...");
        Validator validator = new Validator(brokerConfig);

        // session registry
        logger.debug("Initializing session registry ...");
        SessionRegistry registry = new SessionRegistry();

        // storage
        logger.debug("Initializing redis storage ...");
        RedisSyncStorage redis = (RedisSyncStorage) Class.forName(redisConfig.getString("storage.sync.class")).newInstance();
        redis.init(redisConfig);

        // cluster
        logger.debug("Initializing cluster ...");
        NATSCluster cluster = new NATSCluster();
        cluster.init(clusterConfig, brokerId, registry);

        // authenticator
        logger.debug("Initializing authenticator...");
        Authenticator authenticator = (Authenticator) Class.forName(authenticatorConfig.getString("authenticator.class")).newInstance();
        authenticator.init(authenticatorConfig);

        // broker
        final int keepAlive = brokerConfig.getInt("mqtt.keepalive.default");
        final int keepAliveMax = brokerConfig.getInt("mqtt.keepalive.max");
        final boolean ssl = brokerConfig.getBoolean("mqtt.ssl.enabled");
        final SslContext sslContext = ssl ? SslContextBuilder.forServer(new File(brokerConfig.getString("mqtt.ssl.certPath")), new File(brokerConfig.getString("mqtt.ssl.keyPath")), brokerConfig.getString("mqtt.ssl.keyPassword")).build() : null;
        final String host = brokerConfig.getString("mqtt.host");
        final int port = ssl ? brokerConfig.getInt("mqtt.ssl.port") : brokerConfig.getInt("mqtt.port");

        // tcp server
        logger.debug("Initializing tcp server ...");
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
        EventLoopGroup bossGroup = brokerConfig.getBoolean("netty.useEpoll") ? new EpollEventLoopGroup() : new NioEventLoopGroup();
        EventLoopGroup workerGroup = brokerConfig.getBoolean("netty.useEpoll") ? new EpollEventLoopGroup() : new NioEventLoopGroup();
        // EventLoopGroup handlerGroup = brokerConfig.getBoolean("netty.useEpoll") ? new EpollEventLoopGroup() : new NioEventLoopGroup();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.debug("MQTT broker is shutting down ...");

                workerGroup.shutdownGracefully();
                bossGroup.shutdownGracefully();
                cluster.destroy();
                authenticator.destroy();
                redis.destroy();

                logger.info("MQTT broker has been shut down.");
            }
        });

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(brokerConfig.getBoolean("netty.useEpoll") ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        // ssl
                        if (ssl) {
                            p.addLast("ssl", sslContext.newHandler(ch.alloc()));
                        }
                        // idle
                        p.addFirst("idleHandler", new IdleStateHandler(0, 0, keepAlive));
                        // mqtt encoder & decoder
                        p.addLast("encoder", MqttEncoder.INSTANCE);
                        p.addLast("decoder", new MqttDecoder());
                        // logic handler
                        // p.addLast(handlerGroup, "logicHandler", new SyncRedisHandler(authenticator, cluster, redis, registry, validator, brokerId, keepAlive, keepAliveMax));
                        p.addLast("logicHandler", new SyncRedisHandler(authenticator, cluster, redis, registry, validator, brokerId, keepAlive, keepAliveMax));
                    }
                })
                .option(ChannelOption.SO_BACKLOG, brokerConfig.getInt("netty.soBacklog"))
                .childOption(ChannelOption.SO_KEEPALIVE, brokerConfig.getBoolean("netty.soKeepAlive"));

        // Bind and start to accept incoming connections.
        ChannelFuture f = b.bind(host, port).sync();

        logger.info("MQTT broker is up and running.");

        // Wait until the server socket is closed.
        // Do this to gracefully shut down the server.
        f.channel().closeFuture().sync();
    }
}
