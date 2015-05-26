package io.vertx.ext.cassandra;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

public class CassandraClientTest extends CassandraTestBase {

    private static final String CREATE_KEYSPACE = "create keyspace vertx_cassandra with replication = { 'class': 'SimpleStrategy', 'replication_factor': 2 }";
    private static final String DROP_KEYSPACE = "drop keyspace vertx_cassandra";

    private CassandraClient cassandra;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        JsonObject config = this.getConfig();
        cassandra = CassandraClient.createNonShared(vertx, config);
        this.createKeyspace();
    }

    private void createKeyspace() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        cassandra.execute(CREATE_KEYSPACE, ar -> {
            latch.countDown();
            if (ar.failed()) {
                throw new IllegalStateException("Could not create keyspace for testing", ar.cause());
            }

        });
        latch.await();
    }

    @Override
    public void tearDown() throws Exception {
        if (cassandra != null) {
            this.dropKeyspace();
            cassandra.close();
        }
        super.tearDown();
    }

    private void dropKeyspace() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        cassandra.execute(DROP_KEYSPACE, ar -> {
            latch.countDown();
            if (ar.failed()) {
                throw new IllegalStateException("Could not drop keyspace for testing");
            }

        });
        latch.await();
    }

    private JsonObject getConfig() {
        JsonObject result = new JsonObject().put("contact_points",
                new JsonArray(Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3")));
        result.put("keyspace", "system");

        return result;
    }

    @Test
    public void testCreateTable() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        cassandra
                .execute(
                        "CREATE TABLE vertx_cassandra.timeseries (event_type text, insertion_time timestamp, event text, PRIMARY KEY (event_type, insertion_time)) WITH CLUSTERING ORDER BY (insertion_time DESC)",
                        ar -> {
                            latch.countDown();
                            Assert.assertTrue(ar.succeeded());
                        });
        latch.await();

        int max = 100;
        CountDownLatch latch2 = new CountDownLatch(max);
        long start = System.currentTimeMillis();
        for (int i = 0; i < max; i++) {
            cassandra.execute(
                    "insert into vertx_cassandra.timeseries(event_type, insertion_time, event) values ('An event', dateOf(now()), 'posi "
                            + i + "')",

                    ar -> {
                        latch2.countDown();
                        if (ar.failed()) {
                            Assert.fail(ar.cause().toString());
                        }
                        Assert.assertTrue(ar.succeeded());
                    });
        }
        latch2.await();
        long end = System.currentTimeMillis();
        System.out.println("Inserted in " + (end - start));

    }

    @Test
    public void testQuery() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        cassandra.execute("SELECT * from schema_columns", ar -> {
            Assert.assertTrue(ar.succeeded());
            latch.countDown();
        });
        latch.await();
    }
}
