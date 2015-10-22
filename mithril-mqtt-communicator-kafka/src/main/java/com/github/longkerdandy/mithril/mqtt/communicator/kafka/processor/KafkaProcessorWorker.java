package com.github.longkerdandy.mithril.mqtt.communicator.kafka.processor;

import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorListener;
import com.github.longkerdandy.mithril.mqtt.api.internal.*;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processor Communicator Worker for Kafka
 */
public class KafkaProcessorWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProcessorWorker.class);

    private final KafkaStream<String, InternalMessage> stream;
    private final ProcessorListener listener;

    public KafkaProcessorWorker(KafkaStream<String, InternalMessage> stream, ProcessorListener listener) {
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
                logger.debug("Communicator received: Received {} message from broker {} client {} user {}", msg.getMessageType(), msg.getBrokerId(), msg.getClientId(), msg.getUserName());
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
                        this.listener.onDisconnect(msg);
                        break;
                    default:
                        logger.warn("Internal error: Communicator received unexpected message type {}", msg.getMessageType());
                }
            }
        }
    }
}
