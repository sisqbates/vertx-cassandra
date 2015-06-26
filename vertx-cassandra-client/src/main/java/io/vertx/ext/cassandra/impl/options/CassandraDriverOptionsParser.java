package io.vertx.ext.cassandra.impl.options;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Initializer;

public class CassandraDriverOptionsParser {

    private Cluster.Builder builder;
    private String keyspace;

    public CassandraDriverOptionsParser(JsonObject config) {
        Objects.requireNonNull(config, "Cassandra configuration cannot be null");

        builder = Cluster.builder();
        this.parseContactPoints(config);

        // contactPoints.forEach(c -> builder.addContactPoint((String) c));

        keyspace = config.getString("keyspace");

    }

    private void parseContactPoints(JsonObject config) {
        JsonArray contactPoints = config.getJsonArray("contact_points", new JsonArray());

        List<InetSocketAddress> points = new ArrayList<>(contactPoints.size());
        for (Object point : contactPoints) {
            if (point instanceof String) {
                InetSocketAddress inetAddress;
                try {
                    inetAddress = this.getInetAddress((String) point);
                    points.add(inetAddress);
                } catch (UnknownHostException e) {
                }

            }
        }

        if (points.isEmpty()) {
            throw new IllegalStateException("None of the given contact points is valid");
        }

        builder.addContactPointsWithPorts(points);
    }

    private InetSocketAddress getInetAddress(String hostAndPort) throws UnknownHostException {

        String host = hostAndPort;
        int port = 9042;
        int idx = hostAndPort.indexOf(':');

        if (idx > 0 && idx < hostAndPort.length() - 1) {
            host = hostAndPort.substring(0, idx);
            port = Integer.parseInt(hostAndPort.substring(idx + 1));
        }
        return new InetSocketAddress(InetAddress.getByName(host), port);
    }

    public Initializer configuration() {
        return builder;
    }

    public String keyspace() {
        return keyspace;
    }

}
