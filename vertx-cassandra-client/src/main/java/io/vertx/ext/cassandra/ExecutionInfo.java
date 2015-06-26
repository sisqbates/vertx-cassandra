package io.vertx.ext.cassandra;

import io.vertx.codegen.annotations.Fluent;

public class ExecutionInfo {

    private String queryTrace;

    public String getQueryTrace() {
        return queryTrace;
    }

    @Fluent
    public ExecutionInfo setQueryTrace(String queryTrace) {
        this.queryTrace = queryTrace;
        return this;
    }

}
