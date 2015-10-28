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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for MqttEncoder and MqttDecoder.
 */
public class MqttCodecTest {

    private static final String CLIENT_ID = "RANDOM_TEST_CLIENT";
    private static final String WILL_TOPIC = "/my_will";
    private static final String WILL_MESSAGE = "gone";
    private static final String USER_NAME = "happy_user";
    private static final String PASSWORD = "123_or_no_pwd";

    private static final int KEEP_ALIVE_SECONDS = 600;

    private static final ByteBufAllocator ALLOCATOR = new UnpooledByteBufAllocator(false);

    @Mock
    private final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

    @Mock
    private final Channel channel = mock(Channel.class);

    private final MqttDecoder mqttDecoder = new MqttDecoder();

    private static MqttMessage createMessageWithFixedHeader(MqttMessageType messageType) {
        return new MqttMessage(new MqttFixedHeader(messageType, false, MqttQoS.AT_MOST_ONCE, false, 0));
    }

    private static MqttMessage createMessageWithFixedHeaderAndMessageIdVariableHeader(MqttMessageType messageType) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(
                        messageType,
                        false,
                        messageType == MqttMessageType.PUBREL ? MqttQoS.AT_LEAST_ONCE : MqttQoS.AT_MOST_ONCE,
                        false,
                        0);
        MqttPacketIdVariableHeader mqttPacketIdVariableHeader = MqttPacketIdVariableHeader.from(12345);
        return new MqttMessage(mqttFixedHeader, mqttPacketIdVariableHeader);
    }

    private static MqttConnectMessage createConnectMessage(MqttVersion mqttVersion) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader mqttConnectVariableHeader =
                new MqttConnectVariableHeader(
                        mqttVersion.protocolName(),
                        mqttVersion.protocolLevel(),
                        true,
                        true,
                        true,
                        MqttQoS.AT_LEAST_ONCE,
                        true,
                        true,
                        KEEP_ALIVE_SECONDS);
        MqttConnectPayload mqttConnectPayload =
                new MqttConnectPayload(CLIENT_ID, WILL_TOPIC, WILL_MESSAGE, USER_NAME, PASSWORD);

        return new MqttConnectMessage(mqttFixedHeader, mqttConnectVariableHeader, mqttConnectPayload);
    }

    private static MqttConnAckMessage createConnAckMessage() {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnAckVariableHeader mqttConnAckVariableHeader =
                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, true);
        return new MqttConnAckMessage(mqttFixedHeader, mqttConnAckVariableHeader);
    }

    private static MqttPublishMessage createPublishMessage() {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, true, 0);
        MqttPublishVariableHeader mqttPublishVariableHeader = MqttPublishVariableHeader.from("/abc", 1234);
        ByteBuf payload = ALLOCATOR.buffer();
        payload.writeBytes("whatever".getBytes(CharsetUtil.UTF_8));
        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, payload);
    }

    private static MqttSubscribeMessage createSubscribeMessage() {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, true, 0);
        MqttPacketIdVariableHeader mqttPacketIdVariableHeader = MqttPacketIdVariableHeader.from(12345);

        List<MqttTopicSubscription> topicSubscriptions = new LinkedList<>();
        topicSubscriptions.add(new MqttTopicSubscription("/abc", MqttQoS.AT_LEAST_ONCE));
        topicSubscriptions.add(new MqttTopicSubscription("/def", MqttQoS.AT_LEAST_ONCE));
        topicSubscriptions.add(new MqttTopicSubscription("/xyz", MqttQoS.EXACTLY_ONCE));

        MqttSubscribePayload mqttSubscribePayload = new MqttSubscribePayload(topicSubscriptions);
        return new MqttSubscribeMessage(mqttFixedHeader, mqttPacketIdVariableHeader, mqttSubscribePayload);
    }

    private static MqttSubAckMessage createSubAckMessage() {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttPacketIdVariableHeader mqttPacketIdVariableHeader = MqttPacketIdVariableHeader.from(12345);
        MqttSubAckPayload mqttSubAckPayload = new MqttSubAckPayload(MqttGrantedQoS.SUCCESS_MAX_QOS1, MqttGrantedQoS.SUCCESS_MAX_QOS2, MqttGrantedQoS.SUCCESS_MAX_QOS0);
        return new MqttSubAckMessage(mqttFixedHeader, mqttPacketIdVariableHeader, mqttSubAckPayload);
    }

    private static MqttUnsubscribeMessage createUnsubscribeMessage() {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.UNSUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, true, 0);
        MqttPacketIdVariableHeader mqttPacketIdVariableHeader = MqttPacketIdVariableHeader.from(12345);

        List<String> topics = new LinkedList<>();
        topics.add("/abc");
        topics.add("/def");
        topics.add("/xyz");

        MqttUnsubscribePayload mqttUnsubscribePayload = new MqttUnsubscribePayload(topics);
        return new MqttUnsubscribeMessage(mqttFixedHeader, mqttPacketIdVariableHeader, mqttUnsubscribePayload);
    }

    private static void validateFixedHeaders(MqttFixedHeader expected, MqttFixedHeader actual) {
        assertEquals("MqttFixedHeader MqttMessageType mismatch ", expected.messageType(), actual.messageType());
        assertEquals("MqttFixedHeader Qos mismatch ", expected.qos(), actual.qos());
    }

    private static void validateConnectVariableHeader(
            MqttConnectVariableHeader expected,
            MqttConnectVariableHeader actual) {
        assertEquals("MqttConnectVariableHeader ProtocolName mismatch ", expected.protocolName(), actual.protocolName());
        assertEquals(
                "MqttConnectVariableHeader KeepAlive mismatch ",
                expected.keepAlive(),
                actual.keepAlive());
        assertEquals("MqttConnectVariableHeader ProtocolLevel mismatch ", expected.protocolLevel(), actual.protocolLevel());
        assertEquals("MqttConnectVariableHeader WillQos mismatch ", expected.willQos(), actual.willQos());

        assertEquals("MqttConnectVariableHeader UserNameFlag mismatch ", expected.userNameFlag(), actual.userNameFlag());
        assertEquals("MqttConnectVariableHeader PasswordFlag mismatch ", expected.passwordFlag(), actual.passwordFlag());
        assertEquals(
                "MqttConnectVariableHeader CleanSession mismatch ",
                expected.cleanSession(),
                actual.cleanSession());
        assertEquals("MqttConnectVariableHeader WillFlag mismatch ", expected.willFlag(), actual.willFlag());
        assertEquals(
                "MqttConnectVariableHeader WillRetain mismatch ",
                expected.willRetain(),
                actual.willRetain());
    }

    private static void validateConnectPayload(MqttConnectPayload expected, MqttConnectPayload actual) {
        assertEquals(
                "MqttConnectPayload ClientId mismatch ",
                expected.clientId(),
                actual.clientId());
        assertEquals("MqttConnectPayload UserName mismatch ", expected.userName(), actual.userName());
        assertEquals("MqttConnectPayload Password mismatch ", expected.password(), actual.password());
        assertEquals("MqttConnectPayload WillMessage mismatch ", expected.willMessage(), actual.willMessage());
        assertEquals("MqttConnectPayload WillTopic mismatch ", expected.willTopic(), actual.willTopic());
    }

    private static void validateConnAckVariableHeader(
            MqttConnAckVariableHeader expected,
            MqttConnAckVariableHeader actual) {
        assertEquals(
                "MqttConnAckVariableHeader MqttConnectReturnCode mismatch",
                expected.returnCode(),
                actual.returnCode());
    }

    private static void validatePublishVariableHeader(
            MqttPublishVariableHeader expected,
            MqttPublishVariableHeader actual) {
        assertEquals("MqttPublishVariableHeader TopicName mismatch ", expected.topicName(), actual.topicName());
        assertEquals("MqttPublishVariableHeader PacketId mismatch ", expected.packetId(), actual.packetId());
    }

    private static void validatePublishPayload(ByteBuf expected, ByteBuf actual) {
        assertEquals("PublishPayload mismatch ", 0, expected.compareTo(actual));
    }

    private static void validateMessageIdVariableHeader(
            MqttPacketIdVariableHeader expected,
            MqttPacketIdVariableHeader actual) {
        assertEquals("MqttPacketIdVariableHeader PacketId mismatch ", expected.packetId(), actual.packetId());
    }

    private static void validateSubscribePayload(MqttSubscribePayload expected, MqttSubscribePayload actual) {
        List<MqttTopicSubscription> expectedTopicSubscriptions = expected.subscriptions();
        List<MqttTopicSubscription> actualTopicSubscriptions = actual.subscriptions();

        assertEquals(
                "MqttSubscribePayload TopicSubscriptionList size mismatch ",
                expectedTopicSubscriptions.size(),
                actualTopicSubscriptions.size());
        for (int i = 0; i < expectedTopicSubscriptions.size(); i++) {
            validateTopicSubscription(expectedTopicSubscriptions.get(i), actualTopicSubscriptions.get(i));
        }
    }

    private static void validateTopicSubscription(
            MqttTopicSubscription expected,
            MqttTopicSubscription actual) {
        assertEquals("MqttTopicSubscription Topic mismatch ", expected.topic(), actual.topic());
        assertEquals(
                "MqttTopicSubscription Qos mismatch ",
                expected.requestedQos(),
                actual.requestedQos());
    }

    private static void validateSubAckPayload(MqttSubAckPayload expected, MqttSubAckPayload actual) {
        assertArrayEquals(
                "MqttSubAckPayload GrantedQosLevels mismatch ",
                expected.grantedQoSLevels().toArray(),
                actual.grantedQoSLevels().toArray());
    }

    // Factory methods of different MQTT
    // Message types to help testing

    private static void validateUnsubscribePayload(MqttUnsubscribePayload expected, MqttUnsubscribePayload actual) {
        assertArrayEquals(
                "MqttUnsubscribePayload TopicList mismatch ",
                expected.topics().toArray(),
                actual.topics().toArray());
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(ctx.channel()).thenReturn(channel);
    }

    @Test
    public void testConnectMessageForMqtt31() throws Exception {
        final MqttConnectMessage message = createConnectMessage(MqttVersion.MQTT_3_1);
        ByteBuf byteBuf = MqttEncoder.doEncode(ALLOCATOR, message);

        final List<Object> out = new LinkedList<>();
        mqttDecoder.decode(ctx, byteBuf, out);

        assertEquals("Expected one object bout got " + out.size(), 1, out.size());

        final MqttConnectMessage decodedMessage = (MqttConnectMessage) out.get(0);

        validateFixedHeaders(message.fixedHeader(), decodedMessage.fixedHeader());
        validateConnectVariableHeader(message.variableHeader(), decodedMessage.variableHeader());
        validateConnectPayload(message.payload(), decodedMessage.payload());
    }

    @Test
    public void testConnectMessageForMqtt311() throws Exception {
        final MqttConnectMessage message = createConnectMessage(MqttVersion.MQTT_3_1_1);
        ByteBuf byteBuf = MqttEncoder.doEncode(ALLOCATOR, message);

        final List<Object> out = new LinkedList<>();
        mqttDecoder.decode(ctx, byteBuf, out);

        assertEquals("Expected one object bout got " + out.size(), 1, out.size());

        final MqttConnectMessage decodedMessage = (MqttConnectMessage) out.get(0);

        validateFixedHeaders(message.fixedHeader(), decodedMessage.fixedHeader());
        validateConnectVariableHeader(message.variableHeader(), decodedMessage.variableHeader());
        validateConnectPayload(message.payload(), decodedMessage.payload());
    }

    @Test
    public void testConnAckMessage() throws Exception {
        final MqttConnAckMessage message = createConnAckMessage();
        ByteBuf byteBuf = MqttEncoder.doEncode(ALLOCATOR, message);

        final List<Object> out = new LinkedList<>();
        mqttDecoder.decode(ctx, byteBuf, out);

        assertEquals("Expected one object bout got " + out.size(), 1, out.size());

        final MqttConnAckMessage decodedMessage = (MqttConnAckMessage) out.get(0);
        validateFixedHeaders(message.fixedHeader(), decodedMessage.fixedHeader());
        validateConnAckVariableHeader(message.variableHeader(), decodedMessage.variableHeader());
    }

    @Test
    public void testPublishMessage() throws Exception {
        final MqttPublishMessage message = createPublishMessage();
        ByteBuf byteBuf = MqttEncoder.doEncode(ALLOCATOR, message);

        final List<Object> out = new LinkedList<>();
        mqttDecoder.decode(ctx, byteBuf, out);

        assertEquals("Expected one object bout got " + out.size(), 1, out.size());

        final MqttPublishMessage decodedMessage = (MqttPublishMessage) out.get(0);
        validateFixedHeaders(message.fixedHeader(), decodedMessage.fixedHeader());
        validatePublishVariableHeader(message.variableHeader(), decodedMessage.variableHeader());
        validatePublishPayload(message.payload(), decodedMessage.payload());
    }

    @Test
    public void testPubAckMessage() throws Exception {
        testMessageWithOnlyFixedHeaderAndMessageIdVariableHeader(MqttMessageType.PUBACK);
    }

    @Test
    public void testPubRecMessage() throws Exception {
        testMessageWithOnlyFixedHeaderAndMessageIdVariableHeader(MqttMessageType.PUBREC);
    }

    // Helper methods to compare expected and actual
    // MQTT messages

    @Test
    public void testPubRelMessage() throws Exception {
        testMessageWithOnlyFixedHeaderAndMessageIdVariableHeader(MqttMessageType.PUBREL);
    }

    @Test
    public void testPubCompMessage() throws Exception {
        testMessageWithOnlyFixedHeaderAndMessageIdVariableHeader(MqttMessageType.PUBCOMP);
    }

    @Test
    public void testSubscribeMessage() throws Exception {
        final MqttSubscribeMessage message = createSubscribeMessage();
        ByteBuf byteBuf = MqttEncoder.doEncode(ALLOCATOR, message);

        final List<Object> out = new LinkedList<>();
        mqttDecoder.decode(ctx, byteBuf, out);

        assertEquals("Expected one object bout got " + out.size(), 1, out.size());

        final MqttSubscribeMessage decodedMessage = (MqttSubscribeMessage) out.get(0);
        validateFixedHeaders(message.fixedHeader(), decodedMessage.fixedHeader());
        validateMessageIdVariableHeader(message.variableHeader(), decodedMessage.variableHeader());
        validateSubscribePayload(message.payload(), decodedMessage.payload());
    }

    @Test
    public void testSubAckMessage() throws Exception {
        final MqttSubAckMessage message = createSubAckMessage();
        ByteBuf byteBuf = MqttEncoder.doEncode(ALLOCATOR, message);

        final List<Object> out = new LinkedList<>();
        mqttDecoder.decode(ctx, byteBuf, out);

        assertEquals("Expected one object bout got " + out.size(), 1, out.size());

        final MqttSubAckMessage decodedMessage = (MqttSubAckMessage) out.get(0);
        validateFixedHeaders(message.fixedHeader(), decodedMessage.fixedHeader());
        validateMessageIdVariableHeader(message.variableHeader(), decodedMessage.variableHeader());
        validateSubAckPayload(message.payload(), decodedMessage.payload());
    }

    @Test
    public void testUnSubscribeMessage() throws Exception {
        final MqttUnsubscribeMessage message = createUnsubscribeMessage();
        ByteBuf byteBuf = MqttEncoder.doEncode(ALLOCATOR, message);

        final List<Object> out = new LinkedList<>();
        mqttDecoder.decode(ctx, byteBuf, out);

        assertEquals("Expected one object bout got " + out.size(), 1, out.size());

        final MqttUnsubscribeMessage decodedMessage = (MqttUnsubscribeMessage) out.get(0);
        validateFixedHeaders(message.fixedHeader(), decodedMessage.fixedHeader());
        validateMessageIdVariableHeader(message.variableHeader(), decodedMessage.variableHeader());
        validateUnsubscribePayload(message.payload(), decodedMessage.payload());
    }

    @Test
    public void testUnsubAckMessage() throws Exception {
        testMessageWithOnlyFixedHeaderAndMessageIdVariableHeader(MqttMessageType.UNSUBACK);
    }

    @Test
    public void testPingReqMessage() throws Exception {
        testMessageWithOnlyFixedHeader(MqttMessageType.PINGREQ);
    }

    @Test
    public void testPingRespMessage() throws Exception {
        testMessageWithOnlyFixedHeader(MqttMessageType.PINGRESP);
    }

    @Test
    public void testDisconnectMessage() throws Exception {
        testMessageWithOnlyFixedHeader(MqttMessageType.DISCONNECT);
    }

    private void testMessageWithOnlyFixedHeader(MqttMessageType messageType) throws Exception {
        MqttMessage message = createMessageWithFixedHeader(messageType);
        ByteBuf byteBuf = MqttEncoder.doEncode(ALLOCATOR, message);

        final List<Object> out = new LinkedList<>();
        mqttDecoder.decode(ctx, byteBuf, out);

        assertEquals("Expected one object bout got " + out.size(), 1, out.size());

        final MqttMessage decodedMessage = (MqttMessage) out.get(0);
        validateFixedHeaders(message.fixedHeader(), decodedMessage.fixedHeader());
    }

    private void testMessageWithOnlyFixedHeaderAndMessageIdVariableHeader(MqttMessageType messageType)
            throws Exception {
        MqttMessage message = createMessageWithFixedHeaderAndMessageIdVariableHeader(messageType);

        ByteBuf byteBuf = MqttEncoder.doEncode(ALLOCATOR, message);

        final List<Object> out = new LinkedList<>();
        mqttDecoder.decode(ctx, byteBuf, out);

        assertEquals("Expected one object bout got " + out.size(), 1, out.size());

        final MqttMessage decodedMessage = (MqttMessage) out.get(0);
        validateFixedHeaders(message.fixedHeader(), decodedMessage.fixedHeader());
        validateMessageIdVariableHeader(
                (MqttPacketIdVariableHeader) message.variableHeader(),
                (MqttPacketIdVariableHeader) decodedMessage.variableHeader());
    }
}
