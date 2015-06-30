package io.vertx.ext.cassandra;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject
public class ExecutionOptions {

    public static final int DEFAULT_FETCH_SIZE = -1;

    private boolean tracing = false;
    private ConsistencyLevel consistencyLevel;
    private Long timestamp;
    private int fetchSize = DEFAULT_FETCH_SIZE;
    private boolean idempotent = false;
    private RetryPolicy retryPolicy = RetryPolicy.DEFAULT;
    private ConsistencyLevel serialConsistencyLevel;
    private String pagingState;

    public ExecutionOptions() {
    }

    public ExecutionOptions(ExecutionOptions other) {
        this.tracing = other.tracing;
        this.consistencyLevel = other.consistencyLevel;
        this.timestamp = other.timestamp;
        this.fetchSize = other.fetchSize;
        this.idempotent = other.idempotent;
        this.retryPolicy = other.retryPolicy;
        this.serialConsistencyLevel = other.serialConsistencyLevel;
        this.pagingState = other.pagingState;

    }

    public ExecutionOptions(JsonObject json) {
        this.tracing = json.getBoolean("tracing", false);
        this.consistencyLevel = this.getEnum(json.getString("consistencyLevel"), ConsistencyLevel.class);
        this.timestamp = json.getLong("timestamp", null);
        this.fetchSize = json.getInteger("fetchSize", -1);
        this.idempotent = json.getBoolean("idempotent", false);
        this.retryPolicy = this.getEnum(json.getString("retryPolicy"), RetryPolicy.class);
        this.serialConsistencyLevel = this.getEnum(json.getString("serialConsistencyLevel"), ConsistencyLevel.class);
        this.pagingState = json.getString("pagingState");
    }

    public JsonObject toJson() {
        JsonObject result = new JsonObject();
        result.put("tracing", this.tracing);
        if (this.consistencyLevel != null) {
            result.put("consistencyLevel", this.consistencyLevel.name());
        }
        if (this.timestamp != null) {
            result.put("timestamp", this.timestamp);
        }
        if (this.fetchSize != DEFAULT_FETCH_SIZE) {
            result.put("fetchSize", fetchSize);
        }
        result.put("idempotent", idempotent);
        if (this.retryPolicy != null) {
            result.put("retryPolicy", this.retryPolicy.name());
        }
        if (this.serialConsistencyLevel != null) {
            result.put("serialConsistencyLevel", this.serialConsistencyLevel.name());
        }
        if (this.pagingState != null) {
            result.put("pagingState", this.pagingState);
        }
        return result;
    }

    public boolean isTracing() {
        return tracing;
    }

    public ExecutionOptions setTracing(boolean tracing) {
        this.tracing = tracing;
        return this;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public ExecutionOptions setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public ExecutionOptions setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public ExecutionOptions setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public boolean isIdempotent() {
        return idempotent;
    }

    public ExecutionOptions setIdempotent(boolean idempotent) {
        this.idempotent = idempotent;
        return this;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public ExecutionOptions setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
    }

    public ConsistencyLevel getSerialConsistencyLevel() {
        return serialConsistencyLevel;
    }

    public ExecutionOptions setSerialConsistencyLevel(ConsistencyLevel serialConsistencyLevel) {
        this.serialConsistencyLevel = serialConsistencyLevel;
        return this;
    }

    public String getPagingState() {
        return pagingState;
    }

    public ExecutionOptions setPagingState(String pagingState) {
        this.pagingState = pagingState;
        return this;
    }

    private <T extends Enum<T>> T getEnum(String name, Class<T> enumClass) {
        if (name == null) {
            return null;
        } else {
            return Enum.valueOf(enumClass, name);
        }
    }

}
