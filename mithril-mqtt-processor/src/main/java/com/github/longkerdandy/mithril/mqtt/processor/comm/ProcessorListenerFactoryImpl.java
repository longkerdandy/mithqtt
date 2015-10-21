package com.github.longkerdandy.mithril.mqtt.processor.comm;

import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorCommunicator;
import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorListener;
import com.github.longkerdandy.mithril.mqtt.api.comm.ProcessorListenerFactory;
import com.github.longkerdandy.mithril.mqtt.storage.redis.sync.RedisSyncStorage;

/**
 * Processor Listener Factory Implementation
 */
public class ProcessorListenerFactoryImpl implements ProcessorListenerFactory {

    private final ProcessorCommunicator communicator;
    private final RedisSyncStorage redis;

    public ProcessorListenerFactoryImpl(ProcessorCommunicator communicator, RedisSyncStorage redis) {
        this.communicator = communicator;
        this.redis = redis;
    }

    @Override
    public ProcessorListener newListener() {
        return new ProcessorListenerImpl(this.communicator, this.redis);
    }
}
