package io.vertx.ext.cassandra;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;

public class CassandraTestBase extends VertxTestBase {

    private static final List<String> HOSTS = Arrays.asList("127.0.0.1");

    private static final String CREATE_KEYSPACE = "create keyspace vertx_cassandra with replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }";
    private static final String DROP_KEYSPACE = "drop keyspace if exists vertx_cassandra";

    protected CassandraClient cassandra;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        JsonObject config = this.getConfig();
        config.put("keyspace", "system");

        cassandra = CassandraClient.createNonShared(vertx, config);
        this.setupKeyspace();
        cassandra.close();

        cassandra = CassandraClient.createNonShared(vertx, this.getConfig());
    }

    @Override
    public void tearDown() {
        cassandra.close();
    }

    private void setupKeyspace() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        cassandra.execute(DROP_KEYSPACE, ar -> {
            latch.countDown();
            if (ar.failed()) {
                throw new IllegalStateException("Could not drop keyspace for testing", ar.cause());
            } else {
                cassandra.execute(CREATE_KEYSPACE, ar2 -> {
                    latch.countDown();
                    if (ar2.failed()) {
                        throw new IllegalStateException("Could not create keyspace for testing", ar2.cause());
                    }

                });
            }

        });
        this.awaitLatch(latch);
    }

    protected void executeAndWait(int i, Consumer<Handler<AsyncResult<ResultSet>>> f) throws InterruptedException {
        CountDownLatch l = new CountDownLatch(i);
        f.accept(this.onSuccess(r -> {
            l.countDown();
        }));
        l.await();
    }

    protected JsonObject getConfig() {
        return new JsonObject().put("contact_points", new JsonArray(HOSTS)).put("keyspace", "vertx_cassandra");
    }
}
