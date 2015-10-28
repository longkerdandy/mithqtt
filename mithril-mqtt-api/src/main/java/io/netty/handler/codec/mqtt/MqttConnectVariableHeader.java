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
 * Variable Header for the {@link MqttConnectMessage}
 */
public class MqttConnectVariableHeader {

    protected String protocolName;
    protected int protocolLevel;
    protected boolean userNameFlag;
    protected boolean passwordFlag;
    protected boolean willRetain;
    protected MqttQoS willQos;
    protected boolean willFlag;
    protected boolean cleanSession;
    protected int keepAlive;

    public MqttConnectVariableHeader(
            String protocolName,
            int protocolLevel,
            boolean userNameFlag,
            boolean passwordFlag,
            boolean willRetain,
            MqttQoS willQos,
            boolean willFlag,
            boolean cleanSession,
            int keepAlive) {
        this.protocolName = protocolName;
        this.protocolLevel = protocolLevel;
        this.userNameFlag = userNameFlag;
        this.passwordFlag = passwordFlag;
        this.willRetain = willRetain;
        this.willQos = willQos;
        this.willFlag = willFlag;
        this.cleanSession = cleanSession;
        this.keepAlive = keepAlive;
    }

    public String protocolName() {
        return protocolName;
    }

    public int protocolLevel() {
        return protocolLevel;
    }

    public boolean userNameFlag() {
        return userNameFlag;
    }

    public boolean passwordFlag() {
        return passwordFlag;
    }

    public boolean willRetain() {
        return willRetain;
    }

    public MqttQoS willQos() {
        return willQos;
    }

    public boolean willFlag() {
        return willFlag;
    }

    public boolean cleanSession() {
        return cleanSession;
    }

    public int keepAlive() {
        return keepAlive;
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this)
                + '['
                + "protocolName=" + protocolName
                + ", protocolLevel=" + protocolLevel
                + ", userNameFlag=" + userNameFlag
                + ", passwordFlag=" + passwordFlag
                + ", willRetain=" + willRetain
                + ", willFlag=" + willFlag
                + ", cleanSession=" + cleanSession
                + ", keepAlive=" + keepAlive
                + ']';
    }
}
