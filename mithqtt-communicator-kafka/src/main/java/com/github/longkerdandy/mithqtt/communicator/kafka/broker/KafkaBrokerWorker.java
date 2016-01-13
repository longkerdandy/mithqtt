package com.github.longkerdandy.mithqtt.communicator.kafka.broker;

import com.github.longkerdandy.mithqtt.api.comm.BrokerListener;
import com.github.longkerdandy.mithqtt.api.internal.Disconnect;
import com.github.longkerdandy.mithqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithqtt.api.internal.Publish;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Broker Communicator Worker for Kafka
 */
public class KafkaBrokerWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaBrokerWorker.class);

    private final KafkaConsumer<String, InternalMessage> consumer;
    private final String topic;
    private final BrokerListener listener;

    protected AtomicBoolean closed = new AtomicBoolean(false);

    public KafkaBrokerWorker(KafkaConsumer<String, InternalMessage> consumer, String topic, BrokerListener listener) {
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
