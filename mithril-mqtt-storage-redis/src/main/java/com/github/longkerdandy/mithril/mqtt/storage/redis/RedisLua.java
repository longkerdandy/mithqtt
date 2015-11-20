package com.github.longkerdandy.mithril.mqtt.storage.redis;

/**
 * Redis Lua Script
 */
public class RedisLua {

    // Increments the number stored at key by one with limit
    // Reset to 0 if limit reached (exceeded)
    //
    // Keys 1. Key to be increased
    // Args 1. Maximum number stored at key
    // Returns Number stored at key after increment
    public static final String INCRLIMIT =
            "local cnt = redis.call('INCR', KEYS[1])\n" +
                    "if cnt >= tonumber(ARGV[1])\n" +
                    "then\n" +
                    "   redis.call('SET', KEYS[1], '0')\n" +
                    "end\n" +
                    "return cnt";

    // Insert the specified value at the tail of the list with length limit
    // Removes the elements at the head of the list if limit reached (exceeded)
    //
    // Keys 1. List pushed into
    // Args 1. Value to be pushed
    // Args 2. Maximum length of the list
    // Returns The value popped from the list, or nil
    public static final String RPUSHLIMIT =
            "local cnt = redis.call('RPUSH', KEYS[1], ARGV[1])\n" +
                    "if tonumber(ARGV[2]) > 0 and cnt > tonumber(ARGV[2])\n" +
                    "then\n" +
                    "   return redis.call('LPOP', KEYS[1])\n" +
                    "end\n" +
                    "return nil";

    // Removes the specified key only if its current value is equal to the given value
    //
    // Keys 1. Key to be deleted
    // Args 1. Value to be compared
    // Returns 1 if key is removed, 0 if key untouched
    public static final String CHECKDEL =
            "if ARGV[1] == redis.call('GET', KEYS[1])\n" +
                    "then\n" +
                    "   redis.call('DEL', KEYS[1])\n" +
                    "   return 1\n" +
                    "end\n" +
                    "return 0";
}
