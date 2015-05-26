package io.vertx.ext.cassandra.impl;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.Shareable;
import io.vertx.ext.cassandra.impl.options.CassandraDriverOptionsParser;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraHandle implements Shareable {

    private Vertx vertx;
    private JsonObject configuration;

    private int references = 0;

    private Cluster cluster;
    private Session session;
    private Runnable destroyRunnable;

    public CassandraHandle(Vertx vertx, JsonObject configuration, Runnable destroyRunnable) {
        this.vertx = vertx;
        this.configuration = configuration;
        this.destroyRunnable = destroyRunnable;
    }

    public synchronized Session cassandra() {
        if (session == null) {
            CassandraDriverOptionsParser options = new CassandraDriverOptionsParser(configuration);
            cluster = Cluster.buildFrom(options.configuration());
            session = cluster.connect(options.keyspace());
        }
        return session;
    }

    public synchronized void addReference() {
        references++;
    }

    public synchronized void decreaseReference() {
        references--;
        if (references == 0) {
            if (session != null) {
                session.closeAsync();
            }
            if (cluster != null) {
                cluster.closeAsync();
            }
            destroyRunnable.run();
        }
    }

}
