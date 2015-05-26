package io.vertx.ext.cassandra.impl.options;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Objects;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Initializer;

public class CassandraDriverOptionsParser {

    private Cluster.Builder builder;
    private String keyspace;

    public CassandraDriverOptionsParser(JsonObject config) {
        Objects.requireNonNull(config, "Cassandra configuration cannot be null");

        builder = Cluster.builder();
        JsonArray contactPoints = config.getJsonArray("contact_points", new JsonArray());
        contactPoints.forEach(c -> builder.addContactPoint((String) c));

        keyspace = config.getString("keyspace");

    }

    public Initializer configuration() {
        return builder;
    }

    public String keyspace() {
        return keyspace;
    }

}
