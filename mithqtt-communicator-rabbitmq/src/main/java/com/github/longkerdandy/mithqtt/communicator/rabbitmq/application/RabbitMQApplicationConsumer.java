package com.github.longkerdandy.mithqtt.communicator.rabbitmq.application;

import com.github.longkerdandy.mithqtt.api.comm.ApplicationListener;
import com.github.longkerdandy.mithqtt.api.internal.*;
import com.github.longkerdandy.mithqtt.communicator.rabbitmq.util.JSONs;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Broker Communicator Consumer for RabbitMQ
 */
public class RabbitMQApplicationConsumer extends DefaultConsumer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQApplicationConsumer.class);

    private final ApplicationListener listener;

    /**
     * Constructs a new instance and records its association to the passed-in channel.
     *
     * @param channel  the channel to which this consumer is attached
     * @param listener Application Listener
     */
    public RabbitMQApplicationConsumer(Channel channel, ApplicationListener listener) {
        super(channel);
        this.listener = listener;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        // decode internal message
        InternalMessage msg = JSONs.decodeInternalMessage(body);

        // notify listener
        if (msg != null) {
            logger.debug("Communicator received: Received {} message from broker {} for client {} user {}", msg.getMessageType(), msg.getBrokerId(), msg.getClientId(), msg.getUserName());
            switch (msg.getMessageType()) {
                case CONNECT:
                    this.listener.onConnect((InternalMessage<Connect>) msg);
                    break;
                case PUBLISH:
                    this.listener.onPublish((InternalMessage<Publish>) msg);
                    break;
                case SUBSCRIBE:
                    this.listener.onSubscribe((InternalMessage<Subscribe>) msg);
                    break;
                case UNSUBSCRIBE:
                    this.listener.onUnsubscribe((InternalMessage<Unsubscribe>) msg);
                    break;
                case DISCONNECT:
                    this.listener.onDisconnect((InternalMessage<Disconnect>) msg);
                    break;
                default:
                    logger.warn("Communicator error: Communicator received unexpected message type {}", msg.getMessageType());
            }
        }
    }
}
