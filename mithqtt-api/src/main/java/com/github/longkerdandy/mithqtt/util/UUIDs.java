package com.github.longkerdandy.mithqtt.util;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.NameBasedGenerator;
import com.fasterxml.uuid.impl.RandomBasedGenerator;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * UUID Utils
 */
@SuppressWarnings("unused")
public class UUIDs {

    private UUIDs() {
    }

    /**
     * Generate short version UUID from given uri
     *
     * @param uri Uri/Url
     * @return UUID
     */
    public static String shortUuid(URI uri) {
        NameBasedGenerator generator = Generators.nameBasedGenerator(NameBasedGenerator.NAMESPACE_URL);
        UUID uuid = generator.generate(uri.toString());
        // https://gist.github.com/LeeSanghoon/5811136
        long l = ByteBuffer.wrap(uuid.toString().getBytes()).getLong();
        return Long.toString(l, Character.MAX_RADIX);
    }

    /**
     * Generate short version random UUID
     *
     * @return UUID
     */
    public static String shortUuid() {
        RandomBasedGenerator generator = Generators.randomBasedGenerator();
        UUID uuid = generator.generate();
        // https://gist.github.com/LeeSanghoon/5811136
        long l = ByteBuffer.wrap(uuid.toString().getBytes()).getLong();
        return Long.toString(l, Character.MAX_RADIX);
    }
}