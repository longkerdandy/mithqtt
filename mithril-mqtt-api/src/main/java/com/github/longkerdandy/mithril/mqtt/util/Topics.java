package com.github.longkerdandy.mithril.mqtt.util;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * MQTT Topic Utils
 */
public class Topics {

    // Present an empty level in the wildcard topic filter
    public static final String EMPTY = "~";
    // Present the end of the wildcard topic filter
    public static final String END = "^";

    private Topics() {
    }

    /**
     * Is topic name valid based on MQTT protocol specification and config
     *
     * @param topicName Topic Name
     * @param config    Config
     * @return True if valid
     */
    public static boolean isValidTopicName(String topicName, PropertiesConfiguration config) {
        if (StringUtils.isEmpty(topicName)) return false;
        if (topicName.contains("+")) return false;
        if (topicName.contains("#")) return false;
        // TODO: validate based on config
        return true;
    }

    /**
     * Validate the topic, add EMPTY and END, return as a List of levels
     *
     * @param topic Topic Name or Topic Filter
     * @return List of levels
     */
    public static List<String> sanitize(String topic) {
        if (topic.contains("+") || topic.endsWith("#") || topic.endsWith("#/" + END)) {
            return sanitizeTopicFilter(topic);
        } else {
            return sanitizeTopicName(topic);
        }
    }

    /**
     * Restore sanitized topic to original topic
     *
     * @param topic Topic
     * @return Original Topic Name or Topic Filter
     */
    public static String antidote(String topic) {
        return topic.substring(0, topic.length() - 2).replaceAll(EMPTY, "");
    }

    /**
     * Restore sanitized topic levels to original topic
     *
     * @param topicLevels Topic Levels
     * @return Original Topic Name or Topic Filter
     */
    public static String antidote(List<String> topicLevels) {
        return antidote(String.join("/", topicLevels));
    }

    /**
     * Validate the topic name, add EMPTY and END, return as a List of levels
     * No wildcard allowed!
     *
     * @param topicName Topic Name
     * @return List of levels
     */
    public static List<String> sanitizeTopicName(String topicName) {
        if (StringUtils.isEmpty(topicName)) throw new IllegalArgumentException("Empty topic name");
        if (topicName.contains("+")) throw new IllegalArgumentException("Topic name contains wildcard");
        if (topicName.contains("#")) throw new IllegalArgumentException("Topic name contains wildcard");

        List<String> levels = new ArrayList<>();

        if (topicName.startsWith("/")) topicName = EMPTY + topicName;
        if (topicName.endsWith("/")) topicName = topicName + EMPTY;

        String[] tokens = StringUtils.splitByWholeSeparatorPreserveAllTokens(topicName, "/");
        for (String token : tokens) {
            levels.add(StringUtils.isNotEmpty(token) ? token : EMPTY);
        }

        if (!topicName.endsWith(END)) levels.add(END);

        return levels;
    }

    /**
     * Validate the topic filter, add EMPTY and END, return as a List of levels
     *
     * @param topicFilter Topic Filter
     * @return List of levels
     */
    public static List<String> sanitizeTopicFilter(String topicFilter) {
        if (StringUtils.isEmpty(topicFilter)) throw new IllegalArgumentException("Empty topic filer");
        if (!topicFilter.contains("+") && !topicFilter.contains("#"))
            throw new IllegalArgumentException("Topic filter does not contain wildcard");

        List<String> levels = new ArrayList<>();

        if (topicFilter.startsWith("/")) topicFilter = EMPTY + topicFilter;
        if (topicFilter.endsWith("/")) topicFilter = topicFilter + EMPTY;

        String[] tokens = StringUtils.splitByWholeSeparatorPreserveAllTokens(topicFilter, "/");
        for (int i = 0; i < tokens.length; i++) {
            if (tokens[i].contains("+") && !tokens[i].equals("+"))
                throw new IllegalArgumentException("Illegal topic filter: " + topicFilter);
            if (tokens[i].contains("#") && !tokens[i].equals("#"))
                throw new IllegalArgumentException("Illegal topic filter: " + topicFilter);
            if (tokens[i].equals("#") && !(i == tokens.length - 1 || (i == tokens.length - 2 && END.equals(tokens[i + 1]))))
                throw new IllegalArgumentException("Illegal topic filter: " + topicFilter);
            levels.add(StringUtils.isNotEmpty(tokens[i]) ? tokens[i] : EMPTY);
        }

        if (!topicFilter.endsWith(END)) levels.add(END);

        return levels;
    }
}
