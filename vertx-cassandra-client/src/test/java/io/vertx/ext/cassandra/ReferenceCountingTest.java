package io.vertx.ext.cassandra;

import io.vertx.core.shareddata.LocalMap;

import org.junit.Test;

public class ReferenceCountingTest extends CassandraTestBase {

    private LocalMap<String, Object> getHandlesMap() {
        return vertx.sharedData().getLocalMap("__vertx.CassandraClient.handles");
    }

    @Test
    public void testNonShared() {

        LocalMap<String, Object> handlesMap = this.getHandlesMap();

        CassandraClient c1 = CassandraClient.createNonShared(vertx, this.getConfiguration());
        this.assertEquals(1, handlesMap.size());

        CassandraClient c2 = CassandraClient.createNonShared(vertx, this.getConfiguration());
        this.assertEquals(2, handlesMap.size());

        CassandraClient c3 = CassandraClient.createNonShared(vertx, this.getConfiguration());
        this.assertEquals(3, handlesMap.size());

        c1.close();
        this.assertEquals(2, handlesMap.size());

        c2.close();
        this.assertEquals(1, handlesMap.size());

        c3.close();
        this.assertEquals(0, handlesMap.size());

        this.waitUntil(() -> this.getHandlesMap().isEmpty());
        this.waitUntil(() -> handlesMap != this.getHandlesMap());
    }

    @Test
    public void testDefaultShared() {

        LocalMap<String, Object> handlesMap = this.getHandlesMap();

        CassandraClient c1 = CassandraClient.createShared(vertx, this.getConfiguration());
        this.assertEquals(1, handlesMap.size());

        CassandraClient c2 = CassandraClient.createShared(vertx, this.getConfiguration());
        this.assertEquals(1, handlesMap.size());

        CassandraClient c3 = CassandraClient.createShared(vertx, this.getConfiguration());
        this.assertEquals(1, handlesMap.size());

        c1.close();
        this.assertEquals(1, handlesMap.size());

        c2.close();
        this.assertEquals(1, handlesMap.size());

        c3.close();
        this.assertEquals(0, handlesMap.size());

        this.waitUntil(() -> this.getHandlesMap().isEmpty());
        this.waitUntil(() -> handlesMap != this.getHandlesMap());
    }

    @Test
    public void testNamed() {

        LocalMap<String, Object> handlesMap = this.getHandlesMap();

        CassandraClient c1 = CassandraClient.createShared(vertx, this.getConfiguration(), "1");
        this.assertEquals(1, handlesMap.size());

        CassandraClient c2 = CassandraClient.createShared(vertx, this.getConfiguration(), "2");
        this.assertEquals(2, handlesMap.size());

        CassandraClient c3 = CassandraClient.createShared(vertx, this.getConfiguration(), "3");
        this.assertEquals(3, handlesMap.size());

        c1.close();
        this.assertEquals(2, handlesMap.size());

        c2.close();
        this.assertEquals(1, handlesMap.size());

        c3.close();
        this.assertEquals(0, handlesMap.size());

        this.waitUntil(() -> this.getHandlesMap().isEmpty());
        this.waitUntil(() -> handlesMap != this.getHandlesMap());
    }

    @Test
    public void testMix() {

        LocalMap<String, Object> handlesMap = this.getHandlesMap();

        CassandraClient c1 = CassandraClient.createNonShared(vertx, this.getConfiguration());
        this.assertEquals(1, handlesMap.size());

        CassandraClient c2 = CassandraClient.createShared(vertx, this.getConfiguration(), "1");
        this.assertEquals(2, handlesMap.size());

        CassandraClient c3 = CassandraClient.createShared(vertx, this.getConfiguration(), "2");
        this.assertEquals(3, handlesMap.size());

        CassandraClient c4 = CassandraClient.createShared(vertx, this.getConfiguration(), "2");
        this.assertEquals(3, handlesMap.size());

        c1.close();
        this.assertEquals(2, handlesMap.size());

        c2.close();
        this.assertEquals(1, handlesMap.size());

        c3.close();
        this.assertEquals(1, handlesMap.size());

        c4.close();
        this.assertEquals(0, handlesMap.size());

        this.waitUntil(() -> this.getHandlesMap().isEmpty());
        this.waitUntil(() -> handlesMap != this.getHandlesMap());
    }
}
