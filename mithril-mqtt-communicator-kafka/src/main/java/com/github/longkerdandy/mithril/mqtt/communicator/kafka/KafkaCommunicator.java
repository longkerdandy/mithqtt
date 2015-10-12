package com.github.longkerdandy.mithril.mqtt.communicator.kafka;

import com.github.longkerdandy.mithril.mqtt.api.comm.Communicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorListener;

/**
 * Communicator implementation based on Kafka
 */
public abstract class KafkaCommunicator implements Communicator {

    public Communicator forProcessor(ProcessorListener listener) {
        return null;
    }
}
