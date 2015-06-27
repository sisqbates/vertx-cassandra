package io.vertx.ext.cassandra;

import io.vertx.codegen.annotations.Fluent;

public class ExecutionInfo {

    private String queryTrace;
    private ConsistencyLevel achievedConsistencyLevel;
    private String pagingState;

    public String getQueryTrace() {
        return queryTrace;
    }

    @Fluent
    public ExecutionInfo setQueryTrace(String queryTrace) {
        this.queryTrace = queryTrace;
        return this;
    }

    public ConsistencyLevel getAchievedConsistencyLevel() {
        return achievedConsistencyLevel;
    }

    @Fluent
    public ExecutionInfo setAchievedConsistencyLevel(ConsistencyLevel achievedConsistencyLevel) {
        this.achievedConsistencyLevel = achievedConsistencyLevel;
        return this;
    }

    public String getPagingState() {
        return pagingState;
    }

    @Fluent
    public ExecutionInfo setPagingState(String pagingState) {
        this.pagingState = pagingState;
        return this;
    }

}
