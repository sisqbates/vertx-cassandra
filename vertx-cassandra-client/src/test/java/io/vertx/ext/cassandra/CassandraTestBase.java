package io.vertx.ext.cassandra;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.test.core.VertxTestBase;

import java.util.Arrays;

public class CassandraTestBase extends VertxTestBase {

    protected JsonObject getConfiguration() {
        JsonObject result = new JsonObject().put("contact_points",
                new JsonArray(Arrays.asList("127.0.0.1", "127.0.0.2", "127.0.0.3")));
        result.put("keyspace", "system");

        return result;

    }
}
