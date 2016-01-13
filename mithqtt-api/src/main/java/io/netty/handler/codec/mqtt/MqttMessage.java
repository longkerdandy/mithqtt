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

import io.netty.handler.codec.DecoderResult;
import io.netty.util.internal.StringUtil;

/**
 * Base class for all MQTT message types.
 */
public class MqttMessage {

    protected MqttFixedHeader fixedHeader;
    protected Object variableHeader;
    protected Object payload;
    protected DecoderResult decoderResult;

    public MqttMessage(MqttFixedHeader fixedHeader) {
        this(fixedHeader, null, null);
    }

    public MqttMessage(MqttFixedHeader fixedHeader, Object variableHeader) {
        this(fixedHeader, variableHeader, null);
    }

    public MqttMessage(MqttFixedHeader fixedHeader, Object variableHeader, Object payload) {
        this(fixedHeader, variableHeader, payload, DecoderResult.SUCCESS);
    }

    public MqttMessage(
            MqttFixedHeader fixedHeader,
            Object variableHeader,
            Object payload,
            DecoderResult decoderResult) {
        this.fixedHeader = fixedHeader;
        this.variableHeader = variableHeader;
        this.payload = payload;
        this.decoderResult = decoderResult;
    }

    public MqttFixedHeader fixedHeader() {
        return fixedHeader;
    }

    public Object variableHeader() {
        return variableHeader;
    }

    public Object payload() {
        return payload;
    }

    public DecoderResult decoderResult() {
        return decoderResult;
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this)
                + '['
                + "fixedHeader=" + (fixedHeader != null ? fixedHeader.toString() : "")
                + ", variableHeader=" + (variableHeader != null ? variableHeader.toString() : "")
                + ", payload=" + (payload != null ? payload.toString() : "")
                + ']';
    }
}
