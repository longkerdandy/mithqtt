package com.github.longkerdandy.mithril.mqtt.communicator.kafka.processor;

import com.github.longkerdandy.mithril.mqtt.api.comm.*;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

/**
 * Processor's Communicator Worker
 */
public class ProcessorWorker implements Runnable {

    private final KafkaStream<String, CommunicatorMessage> stream;
    private final ProcessorListener listener;

    public ProcessorWorker(KafkaStream<String, CommunicatorMessage> stream, ProcessorListener listener) {
        this.stream = stream;
        this.listener = listener;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        for (MessageAndMetadata<String, CommunicatorMessage> m : this.stream) {
            CommunicatorMessage msg = m.message();
            switch (msg.getMessageType()) {
                case CONNECT:
                    listener.onConnect((CommunicatorMessage<CommunicatorConnectPayload>) msg);
                    break;
                case PUBLISH:
                    listener.onPublish((CommunicatorMessage<CommunicatorPublishPayload>) msg);
                    break;
                case PUBACK:
                    listener.onPubAck((CommunicatorMessage<CommunicatorPacketIdPayload>) msg);
                    break;
                case PUBREC:
                    listener.onPubRec((CommunicatorMessage<CommunicatorPacketIdPayload>) msg);
                    break;
                case PUBREL:
                    listener.onPubRel((CommunicatorMessage<CommunicatorPacketIdPayload>) msg);
                    break;
                case PUBCOMP:
                    listener.onPubComp((CommunicatorMessage<CommunicatorPacketIdPayload>) msg);
                    break;
                case SUBSCRIBE:
                    listener.onSubscribe((CommunicatorMessage<CommunicatorSubscribePayload>) msg);
                    break;
                case UNSUBSCRIBE:
                    listener.onUnsubscribe((CommunicatorMessage<CommunicatorUnsubscribePayload>) msg);
                    break;
                case DISCONNECT:
                    listener.onDisconnect(msg);
                    break;
            }
        }
    }
}
