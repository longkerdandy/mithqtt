package com.github.longkerdandy.mithril.mqtt.storage.redis.sync;

import com.github.longkerdandy.mithril.mqtt.storage.redis.RedisKey;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.ValueScanCursor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * RedisSyncStandaloneStorage Test
 */
public class RedisSyncStandaloneStorageTest {

    private static RedisSyncStandaloneStorage redis;

    @BeforeClass
    public static void init() {
        redis = new RedisSyncStandaloneStorage(RedisURI.create("redis://localhost"));
        redis.init();
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
}
