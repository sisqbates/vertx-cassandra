package io.vertx.ext.cassandra.impl;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.cassandra.CassandraClient;
import io.vertx.ext.cassandra.ConsistencyLevel;
import io.vertx.ext.cassandra.ExecutionOptions;
import io.vertx.ext.cassandra.ResultSet;
import io.vertx.ext.cassandra.RetryPolicy;
import io.vertx.ext.cassandra.impl.util.DelegateFutureCallback;
import io.vertx.ext.cassandra.impl.util.VertxExecutor;

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
        return this.execute(statement, Collections.emptyList(), resultHandler);
    }

    @Override
    public CassandraClient execute(String statement, List<Object> parameters,
            Handler<AsyncResult<ResultSet>> resultHandler) {
        return this.executeWithOptions(statement, parameters, this.getDefaultExecutionOptions(), resultHandler);
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
    public CassandraClient executeWithOptions(String statement, ExecutionOptions options,
            Handler<AsyncResult<ResultSet>> resultHandler) {

        return this.executeWithOptions(statement, Collections.emptyList(), options, resultHandler);
    }

    @Override
    public CassandraClient executeWithOptions(String statement, List<Object> parameters, ExecutionOptions options,
            Handler<AsyncResult<ResultSet>> resultHandler) {

        Statement stmt = this.buildStatement(statement, parameters, options);

        ResultSetFuture queryFuture = handle.cassandra().executeAsync(stmt);

        Futures.addCallback(queryFuture, this.collectResultSetRows(resultHandler), executor);
        return this;
    }

    private Statement buildStatement(String statement, List<Object> parameters, ExecutionOptions options) {

        SimpleStatement result = new SimpleStatement(statement, parameters.toArray());
        if (options.isTracing())
            result.enableTracing();
        else
            result.disableTracing();

        if (options.getConsistencyLevel() != null)
            result.setConsistencyLevel(
                    com.datastax.driver.core.ConsistencyLevel.valueOf(options.getConsistencyLevel().name()));

        result.setFetchSize(options.getFetchSize());
        result.setIdempotent(options.isIdempotent());

        if (options.getPagingState() != null)
            result.setPagingState(PagingState.fromString(options.getPagingState()));

        if (result.getRetryPolicy() != null)
            result.setRetryPolicy(this.mapRetryPolicyToCassandraClass(options.getRetryPolicy()));

        if (result.getSerialConsistencyLevel() != null)
            result.setSerialConsistencyLevel(
                    com.datastax.driver.core.ConsistencyLevel.valueOf(options.getSerialConsistencyLevel().name()));

        return result;
    }

    private com.datastax.driver.core.policies.RetryPolicy mapRetryPolicyToCassandraClass(RetryPolicy retryPolicy) {
        switch (retryPolicy) {
        case DEFAULT:
            return DefaultRetryPolicy.INSTANCE;
        case DOWNGRADING:
            return DowngradingConsistencyRetryPolicy.INSTANCE;
        case FALLTHROUGH:
            return FallthroughRetryPolicy.INSTANCE;
        default:
            throw new IllegalArgumentException("Unknown RetryPolicy: " + retryPolicy);
        }
    }

    private ExecutionOptions getDefaultExecutionOptions() {
        ExecutionOptions result = new ExecutionOptions();
        result.setConsistencyLevel(ConsistencyLevel.valueOf(QueryOptions.DEFAULT_CONSISTENCY_LEVEL.name()));
        result.setFetchSize(QueryOptions.DEFAULT_FETCH_SIZE);
        result.setSerialConsistencyLevel(
                ConsistencyLevel.valueOf(QueryOptions.DEFAULT_SERIAL_CONSISTENCY_LEVEL.name()));
        result.setIdempotent(QueryOptions.DEFAULT_IDEMPOTENCE);
        return result;
    }

    @Override
    public void close() {
        handle.decreaseReference();
    }

}
