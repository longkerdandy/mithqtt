package com.github.longkerdandy.mithqtt.communicator.rabbitmq.http;

import com.github.longkerdandy.mithqtt.api.comm.HttpCommunicator;
import com.github.longkerdandy.mithqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithqtt.communicator.rabbitmq.ex.RabbitMQExceptionHandler;
import com.github.longkerdandy.mithqtt.communicator.rabbitmq.util.JSONs;
import com.rabbitmq.client.*;
import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Broker Communicator implementation for RabbitMQ
 */
@SuppressWarnings("unused")
public class RabbitMQHttpCommunicator implements HttpCommunicator {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQHttpCommunicator.class);

    // rabbitmq
    protected Connection conn;
    protected Channel channel;

    // broker
    protected String BROKER_TOPIC_PREFIX;

    // application
    protected String APPLICATION_TOPIC;

    @Override
    public void init(AbstractConfiguration config, String serverId) {
        try {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setUsername(config.getString("rabbitmq.userName", ConnectionFactory.DEFAULT_USER));
            cf.setPassword(config.getString("rabbitmq.password", ConnectionFactory.DEFAULT_PASS));
            cf.setVirtualHost(config.getString("rabbitmq.virtualHost", ConnectionFactory.DEFAULT_VHOST));
            cf.setAutomaticRecoveryEnabled(true);
            cf.setExceptionHandler(new RabbitMQExceptionHandler());
            this.conn = cf.newConnection(Address.parseAddresses(config.getString("rabbitmq.addresses")));
            this.channel = conn.createChannel();

            logger.trace("Initializing RabbitMQ broker resources ...");

            BROKER_TOPIC_PREFIX = config.getString("communicator.broker.topic");

            logger.trace("Initializing RabbitMQ application resources ...");

            APPLICATION_TOPIC = config.getString("communicator.application.topic");
            this.channel.exchangeDeclare(APPLICATION_TOPIC, "topic", true);

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

    @Override
    public void sendToBroker(String brokerId, InternalMessage message) {
        String brokerTopic = BROKER_TOPIC_PREFIX + "." + brokerId;
        try {
            // this.channel.exchangeDeclare(brokerTopic, "topic");
            this.channel.basicPublish(brokerTopic, message.getMessageType().name(), MessageProperties.BASIC, JSONs.Mapper.writeValueAsBytes(message));
        } catch (IOException e) {
            logger.warn("Communicator failed: Failed to send message {} to exchange {}: ", message.getMessageType(), brokerTopic, e);
        }
    }

    @Override
    public void sendToApplication(InternalMessage message) {
        try {
            this.channel.basicPublish(APPLICATION_TOPIC, message.getMessageType().name(), MessageProperties.BASIC, JSONs.Mapper.writeValueAsBytes(message));
        } catch (IOException e) {
            logger.warn("Communicator failed: Failed to send message {} to exchange {}: ", message.getMessageType(), APPLICATION_TOPIC, e);
        }
    }
}
