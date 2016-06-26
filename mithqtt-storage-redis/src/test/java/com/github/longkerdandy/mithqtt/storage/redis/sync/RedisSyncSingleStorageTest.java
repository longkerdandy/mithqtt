package com.github.longkerdandy.mithqtt.storage.redis.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.longkerdandy.mithqtt.api.message.Message;
import com.github.longkerdandy.mithqtt.api.message.MqttAdditionalHeader;
import com.github.longkerdandy.mithqtt.api.message.MqttPublishPayload;
import com.github.longkerdandy.mithqtt.storage.redis.RedisKey;
import com.github.longkerdandy.mithqtt.util.Topics;
import io.netty.handler.codec.mqtt.*;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.MapConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.github.longkerdandy.mithqtt.storage.redis.util.JSONs.ObjectMapper;

/**
 * RedisSyncSingleStorage Test
 */
public class RedisSyncSingleStorageTest {

    private static RedisSyncSingleStorage redis;

    @BeforeClass
    public static void init() throws ConfigurationException {
        Map<String, Object> map = new HashMap<>();
        map.put("redis.type", "single");
        map.put("redis.address", "localhost");
        map.put("mqtt.inflight.queue.size", 3);
        map.put("mqtt.qos2.queue.size", 3);
        map.put("mqtt.retain.queue.size", 3);
        MapConfiguration config = new MapConfiguration(map);

        redis = new RedisSyncSingleStorage();
        redis.init(config);
    }

    @AfterClass
    public static void destroy() {
        redis.destroy();
    }

    @After
    public void clear() {
        redis.server().flushdb();
    }

    @Test
    public void connectionTest() {

        assert redis.lock("client2", 1);
        assert redis.lock("client4", 1);
        assert !redis.lock("client3", 0);
        assert !redis.lock("client5", 0);
        assert !redis.release("client3", -1);
        assert !redis.release("client5", -1);

        assert redis.updateConnectedNode("client1", "node1", 30) == null;
        assert redis.updateConnectedNode("client2", "node1", 30) == null;
        assert redis.updateConnectedNode("client3", "node1", 30) == null;
        assert redis.updateConnectedNode("client4", "node1", 30) == null;
        assert redis.updateConnectedNode("client4", "node2", 30).equals("node1");   // overwrite
        assert redis.updateConnectedNode("client5", "node2", 30) == null;
        assert redis.updateConnectedNode("client5", "node2", 30).equals("node2");   // overwrite

        assert !redis.lock("client2", 1);
        assert !redis.lock("client4", 1);
        assert !redis.release("client2", -1);
        assert !redis.release("client4", -1);
        assert redis.release("client2", 2);
        assert redis.release("client4", 2);

        assert redis.getConnectedNode("client1").equals("node1");
        assert redis.getConnectedNode("client2").equals("node1");
        assert redis.getConnectedNode("client3").equals("node1");
        assert redis.getConnectedNode("client4").equals("node2");
        assert redis.getConnectedNode("client5").equals("node2");

        assert redis.lock("client2", 0);
        assert redis.lock("client4", 0);

        assert redis.removeConnectedNode("client2", "node1");
        assert !redis.removeConnectedNode("client4", "node1");   // not exist

        assert !redis.release("client2", -1);   // removed
        assert redis.release("client4", -1);

        assert redis.getConnectedNode("client2") == null;
        assert redis.getConnectedNode("client4").equals("node2");
    }

    @Test
    public void sessionExistTest() {
        assert redis.getSessionExist("client1") == -1;
        redis.updateSessionExist("client1", false);
        assert redis.getSessionExist("client1") == 0;
        redis.updateSessionExist("client1", true);
        assert redis.getSessionExist("client1") == 1;
        redis.removeSessionExist("client1");
        assert redis.getSessionExist("client1") == -1;
    }

    @Test
    public void packetIdTest() {
        assert redis.getNextPacketId("client1") == 1;
        assert redis.getNextPacketId("client1") == 2;
        assert redis.getNextPacketId("client1") == 3;

        redis.string().set(RedisKey.nextPacketId("client1"), "65533");

        assert redis.getNextPacketId("client1") == 65534;
        assert redis.getNextPacketId("client1") == 65535;
        assert redis.getNextPacketId("client1") == 1;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void inFlightTest() throws IOException {
        String json = "{\"menu\": {\n" +
                "  \"id\": \"file\",\n" +
                "  \"value\": \"File\",\n" +
                "  \"popup\": {\n" +
                "    \"menuItem\": [\n" +
                "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" +
                "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
                "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n" +
                "    ]\n" +
                "  }\n" +
                "}}";
        JsonNode jn = ObjectMapper.readTree(json);
        Message<MqttPublishVariableHeader, MqttPublishPayload> publish = new Message<>(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttAdditionalHeader(MqttVersion.MQTT_3_1_1, "client1", "user1", "broker1"),
                MqttPublishVariableHeader.from("menuTopic", 12345),
                new MqttPublishPayload(ObjectMapper.writeValueAsBytes(jn)));

        redis.addInFlightMessage("client1", 12345, publish, false);
        publish = redis.getInFlightMessage("client1", 12345);

        assert publish.fixedHeader().messageType() == MqttMessageType.PUBLISH;
        assert !publish.fixedHeader().dup();
        assert publish.fixedHeader().qos() == MqttQoS.AT_LEAST_ONCE;
        assert !publish.fixedHeader().retain();
        assert publish.additionalHeader().version() == MqttVersion.MQTT_3_1_1;
        assert publish.additionalHeader().clientId().equals("client1");
        assert publish.additionalHeader().userName().equals("user1");
        assert publish.variableHeader().topicName().equals("menuTopic");
        assert publish.variableHeader().packetId() == 12345;
        jn = ObjectMapper.readTree(publish.payload().bytes());
        assert jn.get("menu").get("id").textValue().endsWith("file");
        assert jn.get("menu").get("value").textValue().endsWith("File");
        assert jn.get("menu").get("popup").get("menuItem").get(0).get("value").textValue().equals("New");
        assert jn.get("menu").get("popup").get("menuItem").get(0).get("onclick").textValue().equals("CreateNewDoc()");
        assert jn.get("menu").get("popup").get("menuItem").get(1).get("value").textValue().equals("Open");
        assert jn.get("menu").get("popup").get("menuItem").get(1).get("onclick").textValue().equals("OpenDoc()");
        assert jn.get("menu").get("popup").get("menuItem").get(2).get("value").textValue().equals("Close");
        assert jn.get("menu").get("popup").get("menuItem").get(2).get("onclick").textValue().equals("CloseDoc()");

        publish = redis.getAllInFlightMessages("client1").get(0);
        assert publish.variableHeader().packetId() == 12345;

        redis.removeInFlightMessage("client1", 12345);

        assert redis.getInFlightMessage("client1", 12345) == null;
        assert redis.getAllInFlightMessages("client1").size() == 0;

        Message<MqttPacketIdVariableHeader, Void> pubrel = new Message<>(
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttAdditionalHeader(MqttVersion.MQTT_3_1_1, "client1", "user1", "broker1"),
                MqttPacketIdVariableHeader.from(10000),
                null
        );
        redis.addInFlightMessage("client1", 10000, pubrel, false);

        pubrel = new Message<>(
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttAdditionalHeader(MqttVersion.MQTT_3_1_1, "client1", "user1", "broker1"),
                MqttPacketIdVariableHeader.from(10001),
                null
        );
        redis.addInFlightMessage("client1", 10001, pubrel, false);

        pubrel = new Message<>(
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttAdditionalHeader(MqttVersion.MQTT_3_1_1, "client1", "user1", "broker1"),
                MqttPacketIdVariableHeader.from(10002),
                null
        );
        redis.addInFlightMessage("client1", 10002, pubrel, false);

        assert redis.getAllInFlightMessages("client1").size() == 3;
        pubrel = redis.getAllInFlightMessages("client1").get(1);

        assert pubrel.fixedHeader().messageType() == MqttMessageType.PUBREL;
        assert !pubrel.fixedHeader().dup();
        assert pubrel.fixedHeader().qos() == MqttQoS.AT_LEAST_ONCE;
        assert !pubrel.fixedHeader().retain();
        assert pubrel.additionalHeader().version() == MqttVersion.MQTT_3_1_1;
        assert pubrel.additionalHeader().clientId().equals("client1");
        assert pubrel.additionalHeader().userName().equals("user1");
        assert pubrel.variableHeader().packetId() == 10001;

        pubrel = new Message<>(
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttAdditionalHeader(MqttVersion.MQTT_3_1_1, "client1", "user1", "broker1"),
                MqttPacketIdVariableHeader.from(10003),
                null
        );
        redis.addInFlightMessage("client1", 10003, pubrel, false);

        assert redis.getAllInFlightMessages("client1").size() == 3;
        assert redis.getInFlightMessage("client1", 10000) == null;

        redis.removeAllInFlightMessage("client1");

        assert redis.getInFlightMessage("client1", 10001) == null;
        assert redis.getInFlightMessage("client1", 10002) == null;
        assert redis.getInFlightMessage("client1", 10003) == null;
        assert redis.getAllInFlightMessages("client1").size() == 0;
    }

    @Test
    public void qos2Test() {
        assert redis.addQoS2MessageId("client1", 10000);
        assert redis.addQoS2MessageId("client1", 10001);
        assert redis.addQoS2MessageId("client1", 10002);
        assert !redis.addQoS2MessageId("client1", 10000);

        assert redis.removeQoS2MessageId("client1", 10000);
        assert redis.removeQoS2MessageId("client1", 10001);
        assert redis.removeQoS2MessageId("client1", 10002);
        assert !redis.removeQoS2MessageId("client1", 10001);

        assert redis.addQoS2MessageId("client1", 10003);
        assert redis.addQoS2MessageId("client1", 10004);
        assert redis.addQoS2MessageId("client1", 10005);
        assert redis.addQoS2MessageId("client1", 10006);

        assert !redis.removeQoS2MessageId("client1", 10003);

        redis.removeAllQoS2MessageId("client1");

        assert !redis.removeQoS2MessageId("client1", 10004);
        assert !redis.removeQoS2MessageId("client1", 10005);
        assert !redis.removeQoS2MessageId("client1", 10006);
    }

    @Test
    public void subscriptionTest() {
        redis.updateSubscription("client1", Topics.sanitizeTopicFilter("a/+/e"), MqttQoS.AT_MOST_ONCE);
        redis.updateSubscription("client1", Topics.sanitizeTopicFilter("a/+"), MqttQoS.AT_LEAST_ONCE);
        redis.updateSubscription("client1", Topics.sanitizeTopicName("a/c/e"), MqttQoS.EXACTLY_ONCE);
        redis.updateSubscription("client2", Topics.sanitizeTopicFilter("a/#"), MqttQoS.AT_MOST_ONCE);
        redis.updateSubscription("client2", Topics.sanitizeTopicFilter("a/+"), MqttQoS.AT_LEAST_ONCE);
        redis.updateSubscription("client2", Topics.sanitizeTopicName("a/c/e"), MqttQoS.EXACTLY_ONCE);

        assert redis.getClientSubscriptions("client1").get("a/+/e/" + Topics.END) == MqttQoS.AT_MOST_ONCE;
        assert redis.getClientSubscriptions("client1").get("a/+/" + Topics.END) == MqttQoS.AT_LEAST_ONCE;
        assert redis.getClientSubscriptions("client1").get("a/c/e/" + Topics.END) == MqttQoS.EXACTLY_ONCE;
        assert redis.getClientSubscriptions("client2").get("a/#/" + Topics.END) == MqttQoS.AT_MOST_ONCE;
        assert redis.getClientSubscriptions("client2").get("a/+/" + Topics.END) == MqttQoS.AT_LEAST_ONCE;
        assert redis.getClientSubscriptions("client2").get("a/c/e/" + Topics.END) == MqttQoS.EXACTLY_ONCE;

        assert redis.getTopicSubscriptions(Topics.sanitizeTopicFilter("a/+/e")).get("client1") == MqttQoS.AT_MOST_ONCE;
        assert redis.getTopicSubscriptions(Topics.sanitizeTopicFilter("a/+")).get("client1") == MqttQoS.AT_LEAST_ONCE;
        assert redis.getTopicSubscriptions(Topics.sanitizeTopicFilter("a/+")).get("client2") == MqttQoS.AT_LEAST_ONCE;
        assert redis.getTopicSubscriptions(Topics.sanitizeTopicName("a/c/e")).get("client1") == MqttQoS.EXACTLY_ONCE;
        assert redis.getTopicSubscriptions(Topics.sanitizeTopicName("a/c/e")).get("client2") == MqttQoS.EXACTLY_ONCE;
        assert redis.getTopicSubscriptions(Topics.sanitizeTopicFilter("a/#")).get("client2") == MqttQoS.AT_MOST_ONCE;

        redis.removeSubscription("client1", Topics.sanitizeTopicFilter("a/+"));

        assert !redis.getTopicSubscriptions(Topics.sanitizeTopicFilter("a/+")).containsKey("client1");
        assert !redis.getClientSubscriptions("client1").containsKey("a/+/" + Topics.END);
    }

    @Test
    public void matchTopicFilterTest() {
        redis.updateSubscription("client1", Topics.sanitizeTopicFilter("a/+/e"), MqttQoS.AT_MOST_ONCE);
        redis.updateSubscription("client1", Topics.sanitizeTopicFilter("a/+"), MqttQoS.AT_LEAST_ONCE);
        redis.updateSubscription("client1", Topics.sanitizeTopicFilter("a/c/f/#"), MqttQoS.EXACTLY_ONCE);
        redis.updateSubscription("client2", Topics.sanitizeTopicFilter("a/#"), MqttQoS.AT_MOST_ONCE);
        redis.updateSubscription("client2", Topics.sanitizeTopicFilter("a/c/+/+"), MqttQoS.AT_LEAST_ONCE);
        redis.updateSubscription("client2", Topics.sanitizeTopicFilter("a/d/#"), MqttQoS.EXACTLY_ONCE);
        redis.updateSubscription("client3", Topics.sanitizeTopicName("a/b/c/d"), MqttQoS.AT_LEAST_ONCE);

        Map<String, MqttQoS> result = new HashMap<>();
        redis.getMatchSubscriptions(Topics.sanitizeTopicName("a/c/f"), result);
        assert result.get("client1") == MqttQoS.EXACTLY_ONCE;
        assert result.get("client2") == MqttQoS.AT_MOST_ONCE;
        assert !result.containsKey("client3");

        result.clear();
        redis.getMatchSubscriptions(Topics.sanitizeTopicName("a/d/e"), result);
        assert result.get("client1") == MqttQoS.AT_MOST_ONCE;
        assert result.get("client2") == MqttQoS.EXACTLY_ONCE;
        assert !result.containsKey("client3");

        result.clear();
        redis.getMatchSubscriptions(Topics.sanitizeTopicName("a/b/c/d"), result);
        assert !result.containsKey("client1");
        assert result.get("client2") == MqttQoS.AT_MOST_ONCE;
        assert result.get("client3") == MqttQoS.AT_LEAST_ONCE;
    }

    @Test
    public void retainTest() throws IOException {
        String json = "{\"menu\": {\n" +
                "  \"id\": \"file\",\n" +
                "  \"value\": \"File\",\n" +
                "  \"popup\": {\n" +
                "    \"menuItem\": [\n" +
                "      {\"value\": \"New\", \"onclick\": \"CreateNewDoc()\"},\n" +
                "      {\"value\": \"Open\", \"onclick\": \"OpenDoc()\"},\n" +
                "      {\"value\": \"Close\", \"onclick\": \"CloseDoc()\"}\n" +
                "    ]\n" +
                "  }\n" +
                "}}";
        JsonNode jn = ObjectMapper.readTree(json);
        Message<MqttPublishVariableHeader, MqttPublishPayload> publish = new Message<>(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttAdditionalHeader(MqttVersion.MQTT_3_1_1, "client1", "user1", "broker1"),
                MqttPublishVariableHeader.from("menuTopic", 12345),
                new MqttPublishPayload(ObjectMapper.writeValueAsBytes(jn)));

        redis.addRetainMessage(Topics.sanitize("a/b/c/d"), publish);
        publish = redis.getMatchRetainMessages(Topics.sanitize("a/b/c/d")).get(0);

        assert publish.fixedHeader().messageType() == MqttMessageType.PUBLISH;
        assert !publish.fixedHeader().dup();
        assert publish.fixedHeader().qos() == MqttQoS.AT_LEAST_ONCE;
        assert !publish.fixedHeader().retain();
        assert publish.additionalHeader().version() == MqttVersion.MQTT_3_1_1;
        assert publish.additionalHeader().userName().equals("user1");
        assert publish.variableHeader().topicName().equals("menuTopic");
        jn = ObjectMapper.readTree(publish.payload().bytes());
        assert jn.get("menu").get("id").textValue().endsWith("file");
        assert jn.get("menu").get("value").textValue().endsWith("File");
        assert jn.get("menu").get("popup").get("menuItem").get(0).get("value").textValue().equals("New");
        assert jn.get("menu").get("popup").get("menuItem").get(0).get("onclick").textValue().equals("CreateNewDoc()");
        assert jn.get("menu").get("popup").get("menuItem").get(1).get("value").textValue().equals("Open");
        assert jn.get("menu").get("popup").get("menuItem").get(1).get("onclick").textValue().equals("OpenDoc()");
        assert jn.get("menu").get("popup").get("menuItem").get(2).get("value").textValue().equals("Close");
        assert jn.get("menu").get("popup").get("menuItem").get(2).get("onclick").textValue().equals("CloseDoc()");

        redis.removeAllRetainMessage(Topics.sanitize("a/b/c/d"));
        assert redis.getMatchRetainMessages(Topics.sanitize("a/b/c/d")).size() == 0;

        redis.addRetainMessage(Topics.sanitize("a/b/c/d"), publish);
        redis.addRetainMessage(Topics.sanitize("a/b/c/d"), publish);

        assert redis.getMatchRetainMessages(Topics.sanitize("a/b/c/d")).size() == 2;

        redis.addRetainMessage(Topics.sanitize("a/b/c/d"), publish);
        redis.addRetainMessage(Topics.sanitize("a/b/c/d"), publish);

        assert redis.getMatchRetainMessages(Topics.sanitize("a/b/c/d")).size() == 3;
    }

    @Test
    public void matchRetainTest() {
        Message<MqttPublishVariableHeader, MqttPublishPayload> p1 = new Message<>(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttAdditionalHeader(MqttVersion.MQTT_3_1_1, "client1", "user1", "broker1"),
                MqttPublishVariableHeader.from("foo/bar", 100),
                new MqttPublishPayload("Hello Retain 1".getBytes()));
        Message<MqttPublishVariableHeader, MqttPublishPayload> p2 = new Message<>(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttAdditionalHeader(MqttVersion.MQTT_3_1_1, "client2", "user2", "broker2"),
                MqttPublishVariableHeader.from("foo/bar/zoo", 200),
                new MqttPublishPayload("Hello Retain 2".getBytes()));
        Message<MqttPublishVariableHeader, MqttPublishPayload> p3 = new Message<>(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttAdditionalHeader(MqttVersion.MQTT_3_1_1, "client3", "user3", "broker3"),
                MqttPublishVariableHeader.from("foo/bar/zoo/rar", 300),
                new MqttPublishPayload("Hello Retain 3".getBytes()));
        Message<MqttPublishVariableHeader, MqttPublishPayload> p4 = new Message<>(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttAdditionalHeader(MqttVersion.MQTT_3_1_1, "client4", "user4", "broker4"),
                MqttPublishVariableHeader.from("foo/moo", 400),
                new MqttPublishPayload("Hello Retain 4".getBytes()));
        Message<MqttPublishVariableHeader, MqttPublishPayload> p5 = new Message<>(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttAdditionalHeader(MqttVersion.MQTT_3_1_1, "client5", "user5", "broker5"),
                MqttPublishVariableHeader.from("foo/moo/zoo", 500),
                new MqttPublishPayload("Hello Retain 5".getBytes()));

        redis.addRetainMessage(Topics.sanitize("foo/bar"), p1);
        redis.addRetainMessage(Topics.sanitize("foo/bar/zoo"), p2);
        redis.addRetainMessage(Topics.sanitize("foo/bar/zoo/rar"), p3);
        redis.addRetainMessage(Topics.sanitize("foo/moo"), p4);
        redis.addRetainMessage(Topics.sanitize("foo/moo/zoo"), p5);

        assert redis.getMatchRetainMessages(Topics.sanitize("foo/+")).size() == 2;
        assert redis.getMatchRetainMessages(Topics.sanitize("foo/bar/+")).size() == 1;
        assert redis.getMatchRetainMessages(Topics.sanitize("foo/#")).size() == 5;
        assert redis.getMatchRetainMessages(Topics.sanitize("foo/bar/#")).size() == 3;
        assert redis.getMatchRetainMessages(Topics.sanitize("foo/bar/zoo/#")).size() == 2;
        assert redis.getMatchRetainMessages(Topics.sanitize("foo/bar/zoo/rar/#")).size() == 1;
        assert redis.getMatchRetainMessages(Topics.sanitize("foo/bar/zoo/+")).size() == 1;
        assert redis.getMatchRetainMessages(Topics.sanitize("foo/zoo/#")).size() == 0;
        assert redis.getMatchRetainMessages(Topics.sanitize("foo/+/#")).size() == 5;
        assert redis.getMatchRetainMessages(Topics.sanitize("foo/+/zoo/#")).size() == 3;
        assert redis.getMatchRetainMessages(Topics.sanitize("#")).size() == 5;
    }
}
