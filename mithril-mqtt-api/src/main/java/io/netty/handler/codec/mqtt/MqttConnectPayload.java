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
 * Payload of {@link MqttConnectMessage}
 */
public class MqttConnectPayload {

    protected String clientId;
    protected String willTopic;
    protected String willMessage;
    protected String userName;
    protected String password;

    public MqttConnectPayload(
            String clientId,
            String willTopic,
            String willMessage,
            String userName,
            String password) {
        this.clientId = clientId;
        this.willTopic = willTopic;
        this.willMessage = willMessage;
        this.userName = userName;
        this.password = password;
    }

    public String clientId() {
        return clientId;
    }

    public String willTopic() {
        return willTopic;
    }

    public String willMessage() {
        return willMessage;
    }

    public String userName() {
        return userName;
    }

    public String password() {
        return password;
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this)
                + '['
                + "clientId=" + clientId
                + ", willTopic=" + willTopic
                + ", willMessage=" + willMessage
                + ", userName=" + userName
                + ", password=" + password
                + ']';
    }
}
