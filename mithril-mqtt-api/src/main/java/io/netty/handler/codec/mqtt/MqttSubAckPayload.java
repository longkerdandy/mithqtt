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
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Payload of the {@link MqttSubAckMessage}
 */
public class MqttSubAckPayload {

    protected List<MqttGrantedQoS> grantedQoSLevels;

    public MqttSubAckPayload(MqttGrantedQoS... grantedQoSLevels) {
        if (grantedQoSLevels == null) {
            throw new IllegalArgumentException("Empty grantedQoSLevels");
        }
        List<MqttGrantedQoS> list = new ArrayList<>(grantedQoSLevels.length);
        Collections.addAll(list, grantedQoSLevels);
        this.grantedQoSLevels = Collections.unmodifiableList(list);
    }

    public MqttSubAckPayload(Iterable<MqttGrantedQoS> grantedQoSLevels) {
        if (grantedQoSLevels == null) {
            throw new IllegalArgumentException("Empty grantedQoSLevels");
        }
        List<MqttGrantedQoS> list = new ArrayList<>();
        for (MqttGrantedQoS v : grantedQoSLevels) {
            if (v == null) {
                continue;
            }
            list.add(v);
        }
        this.grantedQoSLevels = Collections.unmodifiableList(list);
    }

    public List<MqttGrantedQoS> grantedQoSLevels() {
        return grantedQoSLevels;
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this)
                + '['
                + "grantedQoSLevels=" + ArrayUtils.toString(grantedQoSLevels)
                + ']';
    }
}
