package com.github.longkerdandy.mithril.mqtt.util;

import org.junit.Test;

import java.util.Arrays;

import static com.github.longkerdandy.mithril.mqtt.util.Topics.EMPTY;
import static com.github.longkerdandy.mithril.mqtt.util.Topics.END;

/**
 * MQTT Topic Utils Test
 */
public class TopicsTest {

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicFilterFail00() {
        Topics.sanitizeTopicFilter(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicFilterFail01() {
        Topics.sanitizeTopicFilter("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicFilterFail02() {
        Topics.sanitizeTopicFilter("abc/def/g+/h");
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicFilterFail03() {
        Topics.sanitizeTopicFilter("abc/def#/g/h");
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicFilterFail05() {
        Topics.sanitizeTopicFilter("abc/def/g/#/h");
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicFilterFail06() {
        Topics.sanitizeTopicFilter("abc/def/g/#/");
    }

    @Test
    public void sanitizeTopicFilterTest() {
        assert Arrays.equals(Topics.sanitizeTopicFilter("abc/+/g/h").toArray(), new String[]{"abc", "+", "g", "h", END});
        assert Arrays.equals(Topics.sanitizeTopicFilter("abc/def/g/#").toArray(), new String[]{"abc", "def", "g", "#", END});
        assert Arrays.equals(Topics.sanitizeTopicFilter("abc/def/#").toArray(), new String[]{"abc", "def", "#", END});
        assert Arrays.equals(Topics.sanitizeTopicFilter("+/+/g/#").toArray(), new String[]{"+", "+", "g", "#", END});
        assert Arrays.equals(Topics.sanitizeTopicFilter("/abc/def/g/#").toArray(), new String[]{EMPTY, "abc", "def", "g", "#", END});
        assert Arrays.equals(Topics.sanitizeTopicFilter("/abc//def/g/#").toArray(), new String[]{EMPTY, "abc", EMPTY, "def", "g", "#", END});
        assert Arrays.equals(Topics.sanitizeTopicFilter("/abc/+/g/h/").toArray(), new String[]{EMPTY, "abc", "+", "g", "h", EMPTY, END});
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicNameFail00() {
        Topics.sanitizeTopicName(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicNameFail01() {
        Topics.sanitizeTopicName("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicNameFail02() {
        Topics.sanitizeTopicName("abc/def/g/+/h");
    }

    @Test(expected = IllegalArgumentException.class)
    public void sanitizeTopicNameFail03() {
        Topics.sanitizeTopicName("abc/def/g/h/#");
    }

    @Test
    public void sanitizeTopicNameTest() {
        assert Arrays.equals(Topics.sanitizeTopicName("abc/def/g/h").toArray(), new String[]{"abc", "def", "g", "h", END});
        assert Arrays.equals(Topics.sanitizeTopicName("/abc/def/g/h").toArray(), new String[]{EMPTY, "abc", "def", "g", "h", END});
        assert Arrays.equals(Topics.sanitizeTopicName("/abc/def/g/h/").toArray(), new String[]{EMPTY, "abc", "def", "g", "h", EMPTY, END});
        assert Arrays.equals(Topics.sanitizeTopicName("/abc/def//g/h").toArray(), new String[]{EMPTY, "abc", "def", EMPTY, "g", "h", END});
    }

    @Test
    public void sanitizeTest() {
        assert Arrays.equals(Topics.sanitize("abc/+/g/h").toArray(), new String[]{"abc", "+", "g", "h", END});
        assert Arrays.equals(Topics.sanitize("abc/def/g/#").toArray(), new String[]{"abc", "def", "g", "#", END});
        assert Arrays.equals(Topics.sanitize("abc/def/#").toArray(), new String[]{"abc", "def", "#", END});
        assert Arrays.equals(Topics.sanitize("+/+/g/#").toArray(), new String[]{"+", "+", "g", "#", END});
        assert Arrays.equals(Topics.sanitize("/abc/def/g/#").toArray(), new String[]{EMPTY, "abc", "def", "g", "#", END});
        assert Arrays.equals(Topics.sanitize("/abc//def/g/#").toArray(), new String[]{EMPTY, "abc", EMPTY, "def", "g", "#", END});
        assert Arrays.equals(Topics.sanitize("/abc/+/g/h/").toArray(), new String[]{EMPTY, "abc", "+", "g", "h", EMPTY, END});

        assert Arrays.equals(Topics.sanitize("abc/def/g/h").toArray(), new String[]{"abc", "def", "g", "h", END});
        assert Arrays.equals(Topics.sanitize("/abc/def/g/h").toArray(), new String[]{EMPTY, "abc", "def", "g", "h", END});
        assert Arrays.equals(Topics.sanitize("/abc/def/g/h/").toArray(), new String[]{EMPTY, "abc", "def", "g", "h", EMPTY, END});
        assert Arrays.equals(Topics.sanitize("/abc/def//g/h").toArray(), new String[]{EMPTY, "abc", "def", EMPTY, "g", "h", END});
    }

    @Test
    public void antidoteTest() {
        assert Topics.antidote(Arrays.asList(new String[]{"abc", "+", "g", "h", END})).equals("abc/+/g/h");
        assert Topics.antidote(Arrays.asList(new String[]{"abc", "def", "g", "#", END})).equals("abc/def/g/#");
        assert Topics.antidote(Arrays.asList(new String[]{"abc", "def", "#", END})).equals("abc/def/#");
        assert Topics.antidote(Arrays.asList(new String[]{"+", "+", "g", "#", END})).equals("+/+/g/#");
        assert Topics.antidote(Arrays.asList(new String[]{EMPTY, "abc", "def", "g", "#", END})).equals("/abc/def/g/#");
        assert Topics.antidote(Arrays.asList(new String[]{EMPTY, "abc", EMPTY, "def", "g", "#", END})).equals("/abc//def/g/#");
        assert Topics.antidote(Arrays.asList(new String[]{EMPTY, "abc", "+", "g", "h", EMPTY, END})).equals("/abc/+/g/h/");

        assert Topics.antidote(Arrays.asList(new String[]{"abc", "def", "g", "h", END})).equals("abc/def/g/h");
        assert Topics.antidote(Arrays.asList(new String[]{EMPTY, "abc", "def", "g", "h", END})).equals("/abc/def/g/h");
        assert Topics.antidote(Arrays.asList(new String[]{EMPTY, "abc", "def", "g", "h", EMPTY, END})).equals("/abc/def/g/h/");
        assert Topics.antidote(Arrays.asList(new String[]{EMPTY, "abc", "def", EMPTY, "g", "h", END})).equals("/abc/def//g/h");
    }
}
