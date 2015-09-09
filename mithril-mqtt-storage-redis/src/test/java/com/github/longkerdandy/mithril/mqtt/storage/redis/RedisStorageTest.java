package com.github.longkerdandy.mithril.mqtt.storage.redis;

import com.github.longkerdandy.mithril.mqtt.util.TopicUtils;
import com.lambdaworks.redis.RedisFuture;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.github.longkerdandy.mithril.mqtt.util.TopicUtils.END;

/**
 * Redis Storage Test
 */
public class RedisStorageTest {

    private static RedisStorage redis;

    @BeforeClass
    public static void init() {
        redis = new RedisStorage("localhost", 6379);
        redis.init();
    }

    @AfterClass
    public static void destroy() {
        redis.destroy();
    }

    @Test
    public void matchTopicFilter() throws ExecutionException, InterruptedException {
        complete(redis.updateTopicFilterSubscription(TopicUtils.sanitizeTopicFilter("a/+/e"), "client1", "0"));
        complete(redis.updateTopicFilterSubscription(TopicUtils.sanitizeTopicFilter("a/+"), "client1", "1"));
        complete(redis.updateTopicFilterSubscription(TopicUtils.sanitizeTopicFilter("a/c/f/#"), "client1", "2"));
        complete(redis.updateTopicFilterSubscription(TopicUtils.sanitizeTopicFilter("a/#"), "client2", "0"));
        complete(redis.updateTopicFilterSubscription(TopicUtils.sanitizeTopicFilter("a/c/+/+"), "client2", "1"));
        complete(redis.updateTopicFilterSubscription(TopicUtils.sanitizeTopicFilter("a/d/#"), "client2", "2"));

        assert redis.getTopicFilterSubscriptions(TopicUtils.sanitizeTopicFilter("a/+/e")).get().get("client1").equals("0");
        assert redis.getTopicFilterSubscriptions(TopicUtils.sanitizeTopicFilter("a/+")).get().get("client1").equals("1");
        assert redis.getTopicFilterSubscriptions(TopicUtils.sanitizeTopicFilter("a/c/f/#")).get().get("client1").equals("2");
        assert redis.getTopicFilterSubscriptions(TopicUtils.sanitizeTopicFilter("a/#")).get().get("client2").equals("0");
        assert redis.getTopicFilterSubscriptions(TopicUtils.sanitizeTopicFilter("a/c/+/+")).get().get("client2").equals("1");
        assert redis.getTopicFilterSubscriptions(TopicUtils.sanitizeTopicFilter("a/d/#")).get().get("client2").equals("2");

        Map<String, String> result = new HashMap<>();
        match(TopicUtils.sanitizeTopicName("a/c/f"), 0, result);
        assert result.get("client1").equals("2");
        assert result.get("client2").equals("0");
    }

    @After
    public void clear() {
        redis.conn.sync().flushdb();
    }

    protected void complete(List<RedisFuture> futures) throws InterruptedException {
        for (RedisFuture future : futures) {
            future.await(10, TimeUnit.SECONDS);
        }
    }

    protected void match(List<String> topicLevels, int index, Map<String, String> result) throws ExecutionException, InterruptedException {
        List<String> children = redis.matchTopicFilterLevel(topicLevels, index).get();
        // last one
        if (children.size() == 2) {
            int c = children.get(0) == null ? 0 : Integer.parseInt(children.get(0)); // char
            int s = children.get(1) == null ? 0 : Integer.parseInt(children.get(1)); // #
            if (c > 0) {
                result.putAll(redis.getTopicFilterSubscriptions(topicLevels).get());
            }
            if (s > 0) {
                List<String> newTopicLevels = topicLevels.subList(0, index);
                newTopicLevels.add("#");
                newTopicLevels.add(END);
                result.putAll(redis.getTopicFilterSubscriptions(newTopicLevels).get());
            }
        }
        // not last one
        else if (children.size() == 3) {
            int c = children.get(0) == null ? 0 : Integer.parseInt(children.get(0)); // char
            int s = children.get(1) == null ? 0 : Integer.parseInt(children.get(1)); // #
            int p = children.get(2) == null ? 0 : Integer.parseInt(children.get(2)); // +
            if (c > 0) {
                match(topicLevels, index + 1, result);
            }
            if (s > 0) {
                List<String> newTopicLevels = topicLevels.subList(0, index);
                newTopicLevels.add("#");
                newTopicLevels.add(END);
                result.putAll(redis.getTopicFilterSubscriptions(newTopicLevels).get());
            }
            if (p > 0) {
                List<String> newTopicLevels = new ArrayList<>(topicLevels);
                newTopicLevels.set(index, "+");
                match(topicLevels, index + 1, result);
            }
        }
    }
}
