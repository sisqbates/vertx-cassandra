package io.vertx.ext.cassandra;

import java.util.List;
import java.util.UUID;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.cassandra.impl.CassandraClientImpl;

public interface CassandraClient {

    static final String DEFAULT_NAME = "DEFAULT";

    static CassandraClient createNonShared(Vertx vertx, JsonObject configuration) {
        return new CassandraClientImpl(vertx, configuration, UUID.randomUUID().toString());
    }

    static CassandraClient createShared(Vertx vertx, JsonObject configuration) {
        return new CassandraClientImpl(vertx, configuration, DEFAULT_NAME);
    }

    static CassandraClient createShared(Vertx vertx, JsonObject configuration, String name) {
        return new CassandraClientImpl(vertx, configuration, name);
    }

    @Fluent
    CassandraClient execute(String statement, Handler<AsyncResult<ResultSet>> resultHandler);

    @Fluent
    CassandraClient execute(String statement, List<Object> parameters, Handler<AsyncResult<ResultSet>> resultHandler);

    @Fluent
    CassandraClient executeWithOptions(String statement, ExecutionOptions options,
            Handler<AsyncResult<ResultSet>> resultHandler);

    @Fluent
    CassandraClient executeWithOptions(String statement, List<Object> parameters, ExecutionOptions options,
            Handler<AsyncResult<ResultSet>> resultHandler);

    void close();

}
