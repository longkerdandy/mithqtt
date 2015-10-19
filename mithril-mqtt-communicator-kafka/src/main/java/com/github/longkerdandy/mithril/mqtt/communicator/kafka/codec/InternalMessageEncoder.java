package com.github.longkerdandy.mithril.mqtt.communicator.kafka.codec;

import com.github.longkerdandy.mithril.mqtt.api.internal.InternalMessage;
import kafka.serializer.Encoder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.github.longkerdandy.mithril.mqtt.communicator.kafka.util.JSONs.Mapper;

/**
 * Internal Message Kafka Encoder
 */
@SuppressWarnings("unused")
public class InternalMessageEncoder implements Encoder<InternalMessage> {

    private static final Logger logger = LoggerFactory.getLogger(InternalMessageEncoder.class);

    @Override
    public byte[] toBytes(InternalMessage internalMessage) {
        try {
            return Mapper.writeValueAsBytes(internalMessage);
        } catch (IOException e) {
            logger.warn("Encode error: Encode internal message with error: {}", ExceptionUtils.getMessage(e));
            return new byte[0];
        }
    }
}
