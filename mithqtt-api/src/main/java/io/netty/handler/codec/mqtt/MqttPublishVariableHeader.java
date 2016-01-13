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
 * Variable Header of the {@link MqttPublishMessage}
 */
public class MqttPublishVariableHeader {

    protected String topicName;
    protected int packetId;

    private MqttPublishVariableHeader(String topicName, int packetId) {
        this.topicName = topicName;
        this.packetId = packetId;
    }

    public static MqttPublishVariableHeader from(String topicName) {
        return new MqttPublishVariableHeader(topicName, 0);
    }

    public static MqttPublishVariableHeader from(String topicName, int packetId) {
        if (packetId < 1 || packetId > 0xffff) {
            throw new IllegalArgumentException("packetId: " + packetId + " (expected: 1 ~ 65535)");
        }
        return new MqttPublishVariableHeader(topicName, packetId);
    }

    public String topicName() {
        return topicName;
    }

    public int packetId() {
        return packetId;
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this)
                + '['
                + "topic=" + topicName
                + ", packetId=" + packetId
                + ']';
    }
}
