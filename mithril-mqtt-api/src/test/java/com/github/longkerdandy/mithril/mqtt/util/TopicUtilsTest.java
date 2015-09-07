package com.github.longkerdandy.mithril.mqtt.util;

import org.junit.Test;

import java.util.Arrays;

import static com.github.longkerdandy.mithril.mqtt.util.TopicUtils.EMPTY;
import static com.github.longkerdandy.mithril.mqtt.util.TopicUtils.END;

/**
 * MQTT Topic Utils Test
 */
public class TopicUtilsTest {

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicFilterFail00() {
        TopicUtils.sanitizeTopicFilter(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicFilterFail01() {
        TopicUtils.sanitizeTopicFilter("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicFilterFail02() {
        TopicUtils.sanitizeTopicFilter("abc/def/g+/h");
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicFilterFail03() {
        TopicUtils.sanitizeTopicFilter("abc/def#/g/h");
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicFilterFail05() {
        TopicUtils.sanitizeTopicFilter("abc/def/g/#/h");
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicFilterFail06() {
        TopicUtils.sanitizeTopicFilter("abc/def/g/#/");
    }

    @Test
    public void sanitizeTopicFilter() {
        assert Arrays.equals(TopicUtils.sanitizeTopicFilter("abc/def/g/h").toArray(), new String[]{"abc", "def", "g", "h", END});
        assert Arrays.equals(TopicUtils.sanitizeTopicFilter("abc/+/g/h").toArray(), new String[]{"abc", "+", "g", "h", END});
        assert Arrays.equals(TopicUtils.sanitizeTopicFilter("abc/def/g/#").toArray(), new String[]{"abc", "def", "g", "#", END});
        assert Arrays.equals(TopicUtils.sanitizeTopicFilter("abc/def/#").toArray(), new String[]{"abc", "def", "#", END});
        assert Arrays.equals(TopicUtils.sanitizeTopicFilter("+/+/g/#").toArray(), new String[]{"+", "+", "g", "#", END});
        assert Arrays.equals(TopicUtils.sanitizeTopicFilter("/abc/def/g/h").toArray(), new String[]{EMPTY, "abc", "def", "g", "h", END});
        assert Arrays.equals(TopicUtils.sanitizeTopicFilter("/abc/def/g/#").toArray(), new String[]{EMPTY, "abc", "def", "g", "#", END});
        assert Arrays.equals(TopicUtils.sanitizeTopicFilter("/abc//def/g/#").toArray(), new String[]{EMPTY, "abc", EMPTY, "def", "g", "#", END});
        assert Arrays.equals(TopicUtils.sanitizeTopicFilter("/abc/+/g/h/").toArray(), new String[]{EMPTY, "abc", "+", "g", "h", EMPTY, END});
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicNameFail00() {
        TopicUtils.sanitizeTopicName(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicNameFail01() {
        TopicUtils.sanitizeTopicName("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicNameFail02() {
        TopicUtils.sanitizeTopicName("abc/def/g/+/h");
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicNameFail03() {
        TopicUtils.sanitizeTopicName("abc/def/g/h/#");
    }

    @Test
    public void sanitizeTopicName() {
        assert Arrays.equals(TopicUtils.sanitizeTopicName("abc/def/g/h").toArray(), new String[]{"abc", "def", "g", "h", END});
        assert Arrays.equals(TopicUtils.sanitizeTopicName("/abc/def/g/h").toArray(), new String[]{EMPTY, "abc", "def", "g", "h", END});
        assert Arrays.equals(TopicUtils.sanitizeTopicName("/abc/def/g/h/").toArray(), new String[]{EMPTY, "abc", "def", "g", "h", EMPTY, END});
        assert Arrays.equals(TopicUtils.sanitizeTopicName("/abc/def//g/h").toArray(), new String[]{EMPTY, "abc", "def", EMPTY, "g", "h", END});
    }
}
