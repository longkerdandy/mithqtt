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

/**
 * Return Code of {@link io.netty.handler.codec.mqtt.MqttSubAckMessage}
 */
public enum MqttGrantedQoS {
    AT_MOST_ONCE(0),
    AT_LEAST_ONCE(1),
    EXACTLY_ONCE(2),
    NOT_GRANTED(0x80);

    private final int value;

    MqttGrantedQoS(int value) {
        this.value = value;
    }

    public static MqttGrantedQoS valueOf(int value) {
        for (MqttGrantedQoS r : values()) {
            if (r.value == value) {
                return r;
            }
        }
        throw new IllegalArgumentException("invalid granted QoS: " + value);
    }

    public int value() {
        return value;
    }
}
