package com.github.longkerdandy.mithril.mqtt.communicator.kafka.application;

import com.github.longkerdandy.mithril.mqtt.api.comm.ApplicationListener;
import com.github.longkerdandy.mithril.mqtt.api.internal.*;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application Communicator Worker for Kafka
 */
public class KafkaApplicationWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaApplicationWorker.class);

    private final KafkaStream<String, InternalMessage> stream;
    private final ApplicationListener listener;

    public KafkaApplicationWorker(KafkaStream<String, InternalMessage> stream, ApplicationListener listener) {
        this.stream = stream;
        this.listener = listener;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        // reads from Kafka until stop
        for (MessageAndMetadata<String, InternalMessage> m : this.stream) {
            InternalMessage msg = m.message();
            if (msg != null) {
                logger.debug("Communicator received: Received {} message from processor {} for client {} user {}", msg.getMessageType(), msg.getBrokerId(), msg.getClientId(), msg.getUserName());
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
}
