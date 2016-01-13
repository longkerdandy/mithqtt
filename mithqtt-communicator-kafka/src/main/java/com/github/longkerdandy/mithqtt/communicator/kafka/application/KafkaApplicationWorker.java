package com.github.longkerdandy.mithqtt.communicator.kafka.application;

import com.github.longkerdandy.mithqtt.api.comm.ApplicationListener;
import com.github.longkerdandy.mithqtt.api.internal.*;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Application Communicator Worker for Kafka
 */
public class KafkaApplicationWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaApplicationWorker.class);

    private final KafkaConsumer<String, InternalMessage> consumer;
    private final String topic;
    private final ApplicationListener listener;

    protected AtomicBoolean closed = new AtomicBoolean(false);

    public KafkaApplicationWorker(KafkaConsumer<String, InternalMessage> consumer, String topic, ApplicationListener listener) {
        this.consumer = consumer;
        this.topic = topic;
        this.listener = listener;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        try {
            // subscribe
            this.consumer.subscribe(Collections.singletonList(this.topic));
            // reads from Kafka until stop
            while (!this.closed.get()) {
                ConsumerRecords<String, InternalMessage> records = this.consumer.poll(1000);
                records.forEach(record -> {
                    InternalMessage msg = record.value();
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
                });
            }
        } catch (WakeupException e) {
            // ignore exception if closing
            if (!this.closed.get())
                logger.error("Communicator error: Consumer has been wakeup unexpectedly: ", e);
        } catch (Exception e) {
            logger.error("Communicator error: Unknown error while consuming from kafka", e);
        } finally {
            this.consumer.close();
        }
    }
}
