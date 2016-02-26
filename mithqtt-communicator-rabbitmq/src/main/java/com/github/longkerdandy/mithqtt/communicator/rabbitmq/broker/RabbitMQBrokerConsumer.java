package com.github.longkerdandy.mithqtt.communicator.rabbitmq.broker;

import com.github.longkerdandy.mithqtt.api.comm.BrokerListener;
import com.github.longkerdandy.mithqtt.api.internal.Disconnect;
import com.github.longkerdandy.mithqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithqtt.api.internal.Publish;
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
public class RabbitMQBrokerConsumer extends DefaultConsumer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQBrokerConsumer.class);

    private final BrokerListener listener;

    /**
     * Constructs a new instance and records its association to the passed-in channel.
     *
     * @param channel  the channel to which this consumer is attached
     * @param listener Broker Listener
     */
    public RabbitMQBrokerConsumer(Channel channel, BrokerListener listener) {
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
            logger.debug("Communicator received: Received {} message for client {} user {}", msg.getMessageType(), msg.getClientId(), msg.getUserName());
            switch (msg.getMessageType()) {
                case PUBLISH:
                    listener.onPublish((InternalMessage<Publish>) msg);
                    break;
                case DISCONNECT:
                    listener.onDisconnect((InternalMessage<Disconnect>) msg);
                    break;
                default:
                    logger.warn("Communicator error: Communicator received unexpected message type {}", msg.getMessageType());
            }
        }
    }
}
