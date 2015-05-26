package io.vertx.ext.cassandra.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.cassandra.CassandraClient;
import io.vertx.ext.cassandra.ResultSet;
import io.vertx.ext.cassandra.impl.util.DelegateFutureCallback;
import io.vertx.ext.cassandra.impl.util.VertxExecutor;

import java.util.Objects;

import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

public class CassandraClientImpl implements CassandraClient {

    private static final String CASSANDRA_HANDLES_MAP = "__vertx.CassandraClient.handles";

    private Vertx vertx;
    private CassandraHandle handle;
    private VertxExecutor executor;

    public CassandraClientImpl(Vertx vertx, JsonObject configuration, String dsName) {
        Objects.requireNonNull(vertx);
        Objects.requireNonNull(configuration);
        Objects.requireNonNull(dsName);

        this.vertx = vertx;
        this.handle = this.lookupHandle(vertx, configuration, dsName);
        this.executor = new VertxExecutor(vertx);
    }

    private CassandraHandle lookupHandle(Vertx vertx, JsonObject configuration, String dsName) {
        synchronized (vertx) {
            LocalMap<String, CassandraHandle> map = vertx.sharedData().getLocalMap(CASSANDRA_HANDLES_MAP);
            CassandraHandle handle = map.get(dsName);
            if (handle == null) {
                handle = new CassandraHandle(vertx, configuration, () -> this.removeHandle(map, dsName));
                map.put(dsName, handle);
            }
            handle.addReference();
            return handle;
        }
    }

    private void removeHandle(LocalMap<String, CassandraHandle> map, String dsName) {
        synchronized (vertx) {
            map.remove(dsName);
            if (map.isEmpty()) {
                map.close();
            }
        }
    }

    @Override
    public CassandraClient execute(String statement, Handler<AsyncResult<ResultSet>> resultHandler) {
        ResultSetFuture queryFuture = handle.cassandra().executeAsync(statement);

        Futures.addCallback(queryFuture, this.collectResultSetRows(resultHandler), executor);
        return this;
    }

    private FutureCallback<com.datastax.driver.core.ResultSet> collectResultSetRows(
            Handler<AsyncResult<ResultSet>> resultHandler) {
        return new DelegateFutureCallback<com.datastax.driver.core.ResultSet>(ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture(new QueryResultsCollector().collectResults(ar.result())));
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public CassandraClient executeWithParams(String statement, JsonObject params,
            Handler<AsyncResult<ResultSet>> resultHandler) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() {
        handle.decreaseReference();
    }

}
