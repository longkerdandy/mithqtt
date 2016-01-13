package com.github.longkerdandy.mithqtt.broker.util;

import org.apache.commons.configuration.MapConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.regex.Pattern;

/**
 * Validator Test
 */
public class ValidatorTest {

    private static Validator validator;

    @BeforeClass
    public static void init() {
        validator = new Validator(new MapConfiguration(new HashMap<>()));
        validator.clientIdPattern = Pattern.compile("^[ -~]+$");
        validator.topicNamePattern = Pattern.compile("^[\\w_ /]*$");
        validator.topicFilterPattern = Pattern.compile("^[\\w_ +#/]*$");
    }

    @Test
    public void isClientIdValidTest() {
        assert validator.isClientIdValid("clientId");
        assert validator.isClientIdValid("client_id");
        assert validator.isClientIdValid("Client Id");
        assert !validator.isClientIdValid("\u041e client id");
    }

    @Test
    public void isTopicValidTest() {
        assert validator.isTopicNameValid("foo");
        assert validator.isTopicNameValid("foo/bar");
        assert validator.isTopicNameValid("foo/bar/woo");
        assert validator.isTopicNameValid("foo/bar/woo/rar");
        assert !validator.isTopicNameValid("foo/#");
        assert !validator.isTopicNameValid("foo/+/woo");
        assert validator.isTopicFilterValid("foo");
        assert validator.isTopicFilterValid("foo/bar");
        assert validator.isTopicFilterValid("foo/#");
        assert validator.isTopicFilterValid("foo/+/woo");
        assert validator.isTopicFilterValid("foo/+/woo/#");
    }
}
