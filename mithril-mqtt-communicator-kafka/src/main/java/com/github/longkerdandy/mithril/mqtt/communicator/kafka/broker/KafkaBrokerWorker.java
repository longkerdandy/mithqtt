package com.github.longkerdandy.mithril.mqtt.communicator.kafka.broker;

import com.github.longkerdandy.mithril.mqtt.api.comm.BrokerListener;
import com.github.longkerdandy.mithril.mqtt.api.internal.Disconnect;
import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithril.mqtt.api.internal.Publish;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker Communicator Worker for Kafka
 */
public class KafkaBrokerWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaBrokerWorker.class);

    private final KafkaStream<String, InternalMessage> stream;
    private final BrokerListener listener;

    public KafkaBrokerWorker(KafkaStream<String, InternalMessage> stream, BrokerListener listener) {
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
                logger.debug("Communicator received: Received {} message for client {}", msg.getMessageType(), msg.getClientId());
                switch (msg.getMessageType()) {
                    case PUBLISH:
                        this.listener.onPublish((InternalMessage<Publish>) msg);
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
