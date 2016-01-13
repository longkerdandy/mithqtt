/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.handler.codec.mqtt;

import io.netty.util.internal.StringUtil;

/**
 * See <a href="http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#fixed-header">
 * MQTTV3.1/fixed-header</a>
 */
public class MqttFixedHeader {

    protected MqttMessageType messageType;
    protected boolean dup;
    protected MqttQoS qos;
    protected boolean retain;
    protected int remainingLength;

    public MqttFixedHeader(
            MqttMessageType messageType,
            boolean dup,
            MqttQoS qos,
            boolean retain,
            int remainingLength) {
        this.messageType = messageType;
        this.dup = dup;
        this.qos = qos;
        this.retain = retain;
        this.remainingLength = remainingLength;
    }

    public MqttMessageType messageType() {
        return messageType;
    }

    public boolean dup() {
        return dup;
    }

    public MqttQoS qos() {
        return qos;
    }

    public boolean retain() {
        return retain;
    }

    public int remainingLength() {
        return remainingLength;
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this)
                + '['
                + "messageType=" + messageType
                + ", dup=" + dup
                + ", qos=" + qos
                + ", retain=" + retain
                + ", remainingLength=" + remainingLength
                + ']';
    }
}
