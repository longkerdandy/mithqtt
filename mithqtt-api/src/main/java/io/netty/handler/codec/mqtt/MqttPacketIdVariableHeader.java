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
 * Variable Header containing only Message Id
 * See <a href="http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#msg-id">MQTTV3.1/msg-id</a>
 */
public final class MqttPacketIdVariableHeader {

    protected int packetId;

    private MqttPacketIdVariableHeader(int packetId) {
        this.packetId = packetId;
    }

    public static MqttPacketIdVariableHeader from(int packetId) {
        if (packetId < 1 || packetId > 0xffff) {
            throw new IllegalArgumentException("packetId: " + packetId + " (expected: 1 ~ 65535)");
        }
        return new MqttPacketIdVariableHeader(packetId);
    }

    public int packetId() {
        return packetId;
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this)
                + '['
                + "packetId=" + packetId
                + ']';
    }
}
