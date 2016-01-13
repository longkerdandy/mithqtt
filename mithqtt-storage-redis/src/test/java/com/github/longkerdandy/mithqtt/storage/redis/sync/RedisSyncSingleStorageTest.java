package com.github.longkerdandy.mithqtt.storage.redis.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.longkerdandy.mithqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithqtt.api.internal.Publish;
import com.github.longkerdandy.mithqtt.api.internal.PacketId;
import com.github.longkerdandy.mithqtt.storage.redis.RedisKey;
import com.github.longkerdandy.mithqtt.util.Topics;
import com.lambdaworks.redis.ValueScanCursor;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
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
    public void connectedTest() {
        assert redis.updateConnectedNode("client1", "node1") == null;
        assert redis.updateConnectedNode("client2", "node1") == null;
        assert redis.updateConnectedNode("client3", "node1") == null;
        assert redis.updateConnectedNode("client4", "node1") == null;
        assert redis.updateConnectedNode("client4", "node2").equals("node1");   // overwrite
        assert redis.updateConnectedNode("client5", "node2") == null;
        assert redis.updateConnectedNode("client5", "node2").equals("node2");   // overwrite

        assert redis.getConnectedNode("client1").equals("node1");
        assert redis.getConnectedNode("client2").equals("node1");
        assert redis.getConnectedNode("client3").equals("node1");
        assert redis.getConnectedNode("client4").equals("node2");
        assert redis.getConnectedNode("client5").equals("node2");

        ValueScanCursor<String> vcs1 = redis.getConnectedClients("node1", "0", 100);
        assert vcs1.getValues().contains("client1");
        assert vcs1.getValues().contains("client2");
        assert vcs1.getValues().contains("client3");
        ValueScanCursor<String> vcs2 = redis.getConnectedClients("node2", "0", 100);
        assert vcs2.getValues().contains("client4");
        assert vcs2.getValues().contains("client5");

        assert redis.removeConnectedNode("client3", "node1");
        assert !redis.removeConnectedNode("client4", "node1");   // not exist

        assert redis.getConnectedNode("client3") == null;
        assert redis.getConnectedNode("client4").equals("node2");

        vcs1 = redis.getConnectedClients("node1", "0", 100);
        assert !vcs1.getValues().contains("client3");
        vcs2 = redis.getConnectedClients("node2", "0", 100);
        assert vcs2.getValues().contains("client4");
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
        InternalMessage<Publish> publish = new InternalMessage<>(
                MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false,
                MqttVersion.MQTT_3_1_1, "client1", "user1", "broker1",
                new Publish("menuTopic", 123456, ObjectMapper.writeValueAsBytes(jn)));

        redis.addInFlightMessage("client1", 123456, publish, false);
        publish = redis.getInFlightMessage("client1", 123456);

        assert publish.getMessageType() == MqttMessageType.PUBLISH;
        assert !publish.isDup();
        assert publish.getQos() == MqttQoS.AT_LEAST_ONCE;
        assert !publish.isRetain();
        assert publish.getVersion() == MqttVersion.MQTT_3_1_1;
        assert publish.getClientId().equals("client1");
        assert publish.getUserName().equals("user1");
        assert publish.getPayload().getTopicName().equals("menuTopic");
        assert publish.getPayload().getPacketId() == 123456;
        jn = ObjectMapper.readTree(publish.getPayload().getPayload());
        assert jn.get("menu").get("id").textValue().endsWith("file");
        assert jn.get("menu").get("value").textValue().endsWith("File");
        assert jn.get("menu").get("popup").get("menuItem").get(0).get("value").textValue().equals("New");
        assert jn.get("menu").get("popup").get("menuItem").get(0).get("onclick").textValue().equals("CreateNewDoc()");
        assert jn.get("menu").get("popup").get("menuItem").get(1).get("value").textValue().equals("Open");
        assert jn.get("menu").get("popup").get("menuItem").get(1).get("onclick").textValue().equals("OpenDoc()");
        assert jn.get("menu").get("popup").get("menuItem").get(2).get("value").textValue().equals("Close");
        assert jn.get("menu").get("popup").get("menuItem").get(2).get("onclick").textValue().equals("CloseDoc()");

        publish = redis.getAllInFlightMessages("client1").get(0);
        assert publish.getPayload().getPacketId() == 123456;

        redis.removeInFlightMessage("client1", 123456);

        assert redis.getInFlightMessage("client1", 123456) == null;
        assert redis.getAllInFlightMessages("client1").size() == 0;

        InternalMessage<PacketId> pubrel = new InternalMessage<>(
                MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false,
                MqttVersion.MQTT_3_1_1, "client1", "user1", "broker1",
                new PacketId(10000));

        redis.addInFlightMessage("client1", 10000, pubrel, false);
        pubrel = new InternalMessage<>(
                MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false,
                MqttVersion.MQTT_3_1_1, "client1", "user1", "broker1",
                new PacketId(10001));
        redis.addInFlightMessage("client1", 10001, pubrel, false);
        pubrel = new InternalMessage<>(
                MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false,
                MqttVersion.MQTT_3_1_1, "client1", "user1", "broker1",
                new PacketId(10002));
        redis.addInFlightMessage("client1", 10002, pubrel, false);

        assert redis.getAllInFlightMessages("client1").size() == 3;
        pubrel = redis.getAllInFlightMessages("client1").get(1);

        assert pubrel.getMessageType() == MqttMessageType.PUBREL;
        assert !pubrel.isDup();
        assert pubrel.getQos() == MqttQoS.AT_LEAST_ONCE;
        assert !pubrel.isRetain();
        assert pubrel.getVersion() == MqttVersion.MQTT_3_1_1;
        assert pubrel.getClientId().equals("client1");
        assert pubrel.getUserName().equals("user1");
        assert pubrel.getPayload().getPacketId() == 10001;

        pubrel = new InternalMessage<>(
                MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false,
                MqttVersion.MQTT_3_1_1, "client1", "user1", "broker1",
                new PacketId(10003));
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
        InternalMessage<Publish> publish = new InternalMessage<>(
                MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false,
                MqttVersion.MQTT_3_1_1, "client1", "user1", "broker1",
                new Publish("menuTopic", 123456, ObjectMapper.writeValueAsBytes(jn)));

        redis.addRetainMessage(Topics.sanitize("a/b/c/d"), publish);
        publish = redis.getMatchRetainMessages(Topics.sanitize("a/b/c/d")).get(0);

        assert publish.getMessageType() == MqttMessageType.PUBLISH;
        assert !publish.isDup();
        assert publish.getQos() == MqttQoS.AT_LEAST_ONCE;
        assert !publish.isRetain();
        assert publish.getVersion() == MqttVersion.MQTT_3_1_1;
        assert publish.getUserName().equals("user1");
        assert publish.getPayload().getTopicName().equals("menuTopic");
        jn = ObjectMapper.readTree(publish.getPayload().getPayload());
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
        InternalMessage<Publish> p1 = new InternalMessage<>(
                MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false,
                MqttVersion.MQTT_3_1_1, "client1", "user1", "broker1",
                new Publish("foo/bar", 100, "Hello Retain 1".getBytes()));
        InternalMessage<Publish> p2 = new InternalMessage<>(
                MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false,
                MqttVersion.MQTT_3_1_1, "client2", "user2", "broker2",
                new Publish("foo/bar/zoo", 200, "Hello Retain 2".getBytes()));
        InternalMessage<Publish> p3 = new InternalMessage<>(
                MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false,
                MqttVersion.MQTT_3_1_1, "client3", "user3", "broker3",
                new Publish("foo/bar/zoo/rar", 300, "Hello Retain 3".getBytes()));
        InternalMessage<Publish> p4 = new InternalMessage<>(
                MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false,
                MqttVersion.MQTT_3_1_1, "client4", "user4", "broker4",
                new Publish("foo/moo", 400, "Hello Retain 4".getBytes()));
        InternalMessage<Publish> p5 = new InternalMessage<>(
                MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false,
                MqttVersion.MQTT_3_1_1, "client5", "user5", "broker5",
                new Publish("foo/moo/zoo", 500, "Hello Retain 5".getBytes()));

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
