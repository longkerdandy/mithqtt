package com.github.longkerdandy.mithqtt.communicator.rabbitmq.application;

import com.github.longkerdandy.mithqtt.api.comm.ApplicationCommunicator;
import com.github.longkerdandy.mithqtt.api.comm.ApplicationListenerFactory;
import com.github.longkerdandy.mithqtt.communicator.rabbitmq.ex.RabbitMQExceptionHandler;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Application Communicator implementation for RabbitMQ
 */
@SuppressWarnings("unused")
public class RabbitMQApplicationCommunicator implements ApplicationCommunicator {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQApplicationCommunicator.class);

    // rabbitmq
    protected Connection conn;
    protected Channel channel;

    // application
    protected String APPLICATION_TOPIC;

    @Override
    public void init(AbstractConfiguration config, ApplicationListenerFactory factory) {
        try {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setUsername(config.getString("rabbitmq.userName", ConnectionFactory.DEFAULT_USER));
            cf.setPassword(config.getString("rabbitmq.password", ConnectionFactory.DEFAULT_PASS));
            cf.setVirtualHost(config.getString("rabbitmq.virtualHost", ConnectionFactory.DEFAULT_VHOST));
            cf.setAutomaticRecoveryEnabled(true);
            cf.setExceptionHandler(new RabbitMQExceptionHandler());
            this.conn = cf.newConnection(Address.parseAddresses(config.getString("rabbitmq.addresses")));
            this.channel = conn.createChannel();

            logger.trace("Initializing RabbitMQ application resources ...");

            APPLICATION_TOPIC = config.getString("communicator.application.topic");
            this.channel.exchangeDeclare(APPLICATION_TOPIC, "topic", true);

            logger.trace("Initializing RabbitMQ application consumer's workers ...");

            Channel consumerChan = this.conn.createChannel();
            consumerChan.queueDeclare(config.getString("rabbitmq.app.queueName"), true, false, true, null);
            consumerChan.queueBind(config.getString("rabbitmq.app.queueName"), APPLICATION_TOPIC, config.getString("rabbitmq.app.routingKey"));
            consumerChan.basicConsume(config.getString("rabbitmq.app.queueName"), true, new RabbitMQApplicationConsumer(consumerChan, factory.newListener()));

        } catch (IOException | TimeoutException e) {
            logger.error("Failed to connect to RabbitMQ servers", e);
            throw new IllegalStateException("Init RabbitMQ communicator failed");
        }
    }

    @Override
    public void destroy() {
        try {
            if (this.conn != null) this.conn.close();
        } catch (IOException e) {
            logger.warn("Communicator error: Exception closing the RabbitMQ connection, exiting uncleanly", e);
        }
    }
}
